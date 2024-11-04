// Helper function to generate a UUID v4
function generateUUIDv4() {
    const hexDigits = "0123456789abcdef";
    let uuid = "";
    for (let i = 0; i < 36; i++) {
        if (i === 8 || i === 13 || i === 18 || i === 23) {
            uuid += "-";
        } else if (i === 14) {
            uuid += "4"; // set the version to 4
        } else if (i === 19) {
            uuid += hexDigits.substr((Math.random() * 4) | 8, 1); // set the variant
        } else {
            uuid += hexDigits.charAt(Math.floor(Math.random() * 16));
        }
    }
    return uuid;
}

// type in the below command
// use crdc-datahub


// Insert the document with specified fields
db.configuration.insertMany(
    [{
        _id: generateUUIDv4(),
        type: "TIER",
        keys: { tier: "dev" }
    },
    {
        _id: generateUUIDv4(),
        type: "LOADER_QUEUE",
        keys: {"sqs": "crdcdh-queue-pgu.fifo"}
    },
    {
        _id: generateUUIDv4(),
        type: "FILE_QUEUE",
        keys: {"sqs": "crdcdh-queue-pgu.fifo"}
    },
    {
        _id: generateUUIDv4(),
        type: "METADATA_QUEUE",
        keys: {"sqs": "crdcdh-queue-pgu.fifo"}
    },
    {
        _id: generateUUIDv4(),
        type: "EXPORTER_QUEUE",
        keys: {"sqs": "crdcdh-queue-pgu.fifo"}
    },
    {
        _id: generateUUIDv4(),
        type: "DM_BUCKET_NAME",
        keys: {"dm_bucket": "crdc-hub-dev"}
    },
    {
        _id: generateUUIDv4(),
        type: "DATASYNC_ROLE_ARN",
        keys: {"datasync_role": "arn:aws:iam::420434175168:role/DataSyncS3Role"}
    }]
);
