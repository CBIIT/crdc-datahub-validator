# Batched Metadata Validation Interface

Interface contract between the backend (SQS producer) and the validation service (SQS consumer) for batched metadata validation.

---

## SQS Messages

### Queue

FIFO queue configured via the `METADATA_QUEUE` environment variable. `MessageGroupId` is the `submissionID`; `MessageDeduplicationId` is a unique UUID (`v4()`) generated per message inside `aws-request.js`, regardless of the value passed by the caller.

### Message Types

| Type | Constant | Description |
|------|----------|-------------|
| `"Validate Metadata Batch"` | `TYPE_METADATA_VALIDATE_BATCH` (validator) / `VALIDATION.BATCH_MESSAGE_TYPE` (backend) | Batched flow (one message per chunk of records) |
| `"Validate Metadata"` | `TYPE_METADATA_VALIDATE` | Legacy single-message flow (backward-compatible) |

### Batch Message Fields

```json
{
  "type": "Validate Metadata Batch",
  "validationID": "<string UUID>",
  "submissionID": "<string UUID>",
  "scope": "new" | "all",
  "dataRecordIds": ["<string UUID>", ...],
  "totalBatches": 3,
  "batchIndex": 0
}
```

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `type` | string | yes | Must be `"Validate Metadata Batch"` |
| `validationID` | string (UUID) | yes | Must match `_id` of an existing validation document. Message is rejected if missing; validation will appear stuck. |
| `submissionID` | string (UUID) | yes | Message is silently skipped if missing. |
| `scope` | string | yes | `"new"` or `"all"` (case-insensitive). Must be truthy. |
| `dataRecordIds` | string[] | yes | Array of `dataRecords._id` values. Must be non-empty. |
| `totalBatches` | int | yes | Total number of batch messages for this validation run. Must be >= 1. All messages in a run must carry the same value. |
| `batchIndex` | int | yes | Zero-based index of this batch within the run. |

### Legacy (Non-Batch) Message Fields

```json
{
  "type": "Validate Metadata",
  "validationID": "<string UUID>",
  "submissionID": "<string UUID>",
  "scope": "New" | "All"
}
```

No `dataRecordIds`, `totalBatches`, or `batchIndex`. The validator fetches records internally.

---

## Record Selection and Batching (Backend)

### Scope-Based Record Query

The backend selects `dataRecordIds` from the `dataRecords` collection based on `scope`:

| Scope | Query | Records returned |
|-------|-------|-----------------|
| `"new"` | `{ submissionID, status: "New" }` | Only records with status `"New"` |
| `"all"` | `{ submissionID }` | All records for the submission |

Only the `_id` field is projected; these IDs become `dataRecordIds` in the batch messages.

### Batch Size

Records are chunked into batches. Size is configurable via a `configuration` collection entry with `type: "METADATA_VALIDATION_BATCH_SIZE"` and a `size` field (integer).

| Parameter | Value |
|-----------|-------|
| Config type | `"METADATA_VALIDATION_BATCH_SIZE"` |
| Config field | `size` (Int) |
| Default | **1000** (used when config is missing or `size` is falsy) |
| Minimum | **100** (clamped with logged error if configured below) |
| Maximum | **5000** (based on SQS 256KB message limit with ~27% headroom) |

If the configured `size` exceeds 5000, the backend logs an error and clamps to 5000. If it is below 100, the backend logs an error and clamps to 100.

---

## Database Updates

### Pre-conditions (Backend Responsibility)

Before sending any SQS messages, the backend must:

1. **Set submission status** on the `submissions` document:

   | Field | Value |
   |-------|-------|
   | `metadataValidationStatus` | `"Validating"` |
   | `fileValidationStatus` | `"Validating"` (if file validation also requested) |
   | `crossSubmissionStatus` | `"Validating"` (if cross-submission also requested) |

2. **Create the validation document** in the `validation` collection with at least:

   | Field | Value |
   |-------|-------|
   | `_id` | string UUID |
   | `submissionID` | string UUID |
   | `type` | `["metadata"]`, `["metadata", "file"]`, etc. (always an array) |
   | `scope` | `"new"` or `"all"` |
   | `started` | `Date` |
   | `status` | `"Validating"` |

3. **Send all batch messages** to SQS.

4. **Update the validation document** with `totalBatches` (and optionally `status`/`statusDetail` on failure). This happens **after** all SQS messages are sent, so batches may begin processing before `totalBatches` is written. The validator uses `totalBatches` from the **message**, not the document, so this is safe.

5. **Record validation metadata** on the `submissions` document:

   | Field | Value |
   |-------|-------|
   | `validationStarted` | `Date` |
   | `validationEnded` | `null` |
   | `validationType` | `["metadata"]`, `["file"]`, etc. (lowercased) |
   | `validationScope` | `"new"` or `"all"` (lowercased) |

### Validator Updates Per Batch

On each batch message, the validator atomically updates the **validation document** via `find_one_and_update`:

| Operation | Field | Description |
|-----------|-------|-------------|
| `$inc` | `completedBatches` | +1 per batch |
| `$inc` | `failedBatches` | +1 if the batch failed |
| `$max` | `worstBatchStatus` | Numeric precedence: Passed=0, Warning=1, Error=2 |
| `$push` | `batchStatusDetails` | Failure message string (only for failed batches) |

Completion is detected when `completedBatches >= totalBatches` (from the message, not the document).

### Validator Updates on Final Batch

When the last batch completes, the validator updates both collections in sequence:

**Validation document** (`$set`):

| Operation | Fields |
|-----------|--------|
| `$set` | `metadataStatus`, `metadataEnded`, `status` (if sole type), `ended`, `statusDetail` |

Batch-tracking fields (`completedBatches`, `failedBatches`, `batchStatusDetails`, `worstBatchStatus`) are **retained** on the validation document after completion. Since validation documents are never reused, these fields serve as a historical record of how the batched run progressed.

If the validation document's `type` array has more than one entry, overall `status` and `ended` are deferred until both metadata and file validation have finished. The worst of the two determines the overall status.

**Submission document** (`$set`):

| Field | Value |
|-------|-------|
| `metadataValidationStatus` | `"Passed"`, `"Warning"`, or `"Error"` (re-derived from data record statuses in the DB) |
| `validationEnded` | timestamp |
| `statusDetail` | `[string]` of failure messages, or `null` if all batches passed |
| `updatedAt` | timestamp |

### `statusDetail` Format

- **Batch runs:** `[string]` -- one entry per failed batch describing the failure. `null` when all batches pass.
- **Non-batch runs:** `null` (not set).
- Written to both the `validation` and `submissions` documents under the key `"statusDetail"`.

### Terminal Status Values

| Status | Meaning |
|--------|---------|
| `"Passed"` | All records valid |
| `"Warning"` | Warnings found, no errors |
| `"Error"` | Errors found, or bad input (missing submission, scope, records, model, etc.) |

---

## Data Flow

```
Backend                          SQS (FIFO)                    Validator
  |                                                               |
  |-- set submission "Validating" -> submissions collection       |
  |-- create validation doc -----> validation collection          |
  |-- send batch msg 0 ----------> queue -----------------------> |
  |-- send batch msg 1 ----------> queue -----------------------> |-- validate records
  |-- send batch msg N ----------> queue -----------------------> |-- $inc completedBatches
  |-- update totalBatches -------> validation collection          |
  |-- record validation metadata -> submissions collection        |
  |                                                               |
  |                                          (last batch)         |-- $set final status
  |                                                               |-- update submission status
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Missing `validationID` | Message rejected, no DB update. Validation appears stuck. |
| `totalBatches` < 1 | Message rejected, no DB update. Validation appears stuck. |
| Missing `scope` or empty `dataRecordIds` | Batch marked as failed, counter still incremented. |
| Submission/model/study not found | Batch marked as failed, counter still incremented. |
| `validate_nodes` exception | Batch marked as failed, counter still incremented. |
| Partial `dataRecordIds` match | Warning logged, continues with found records. |
| Validation document not found | `increment_completed_batches` returns `None`; finalization skipped. |
| Backend partial send failure | Backend sets `status: "Error"` and `statusDetail: ["Failed to enqueue {N} of {total} batch messages"]` on validation doc. Validator processes arrived batches but never reaches `completedBatches >= totalBatches`, so it does not finalize. |
| Backend total send failure | Backend rolls back submission validation statuses to their previous values and sets `status: "Error"` / `ended` on the validation doc. |
| Zero total records | Backend does not send messages. Rolls back `metadataValidationStatus` to `null` (no metadata to validate). |
| Zero new records (scope `"new"`) | Backend does not send messages. Preserves the previous `metadataValidationStatus` (nothing new to validate; existing status is still valid). |

---

## Schema Gap: Prisma Validation Model

The validator writes batch-tracking fields (`completedBatches`, `failedBatches`, `batchStatusDetails`, `worstBatchStatus`) directly to MongoDB. These fields are exposed in the **GraphQL schema** (`Validation` type) for frontend progress tracking, but are **missing from the Prisma `Validation` model**. This means Prisma-based queries will not return them. Since validation documents are never reused, these fields persist as a historical record after completion. If the frontend or reporting tools need to access them, either:

- Add these fields to the Prisma schema as optional, or
- Use a raw MongoDB query for validation reads.
