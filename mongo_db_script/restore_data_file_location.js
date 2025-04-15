// Use the appropriate database
// use crdc-datahub2;
// function, getDataFileUrl(submissionID, fileName), to generate a s3 url for a file based on submission.bucketName, submission.rootPath and fileName
function getDataFileUrl(submissionID, fileName) {
    const submission = db.submissions.findOne({ _id: submissionID });
    if (!submission || !submission?.bucketName || !submission?.rootPath || !fileName) {
        return null;
    }
    const bucketName = submission.bucketName;
    const rootPath = submission.rootPath.trim("/");
    const s3Url = `s3://${bucketName}/${rootPath}/file/${fileName}`;
    return s3Url;
}
  
// Function to restore application version based on status
function restoreFileLocation() {
    const bulkOps = [];
    let matchedCount = 0;
    let updatedCount = 0;
    print("\n");
    print("----------------------");
    console.log(`${new Date()} -> Restoring data field: "dataFileLocation"`);
    db.release.find(
        { "nodeType": {"$in": ["file", "data_file", "clinical_measure_file", "cytogenomic_file","methylation_array_file", "pathology_file","radiology_file", "sequencing_file"]}, 
            "dataFileLocation": {$exists: false}, 
            "$or": [{"props.file_name": {$exists: true, $ne: null}}, {"props.data_file_name": {$exists: true, $ne: null}}]
        }).forEach(doc => {
        matchedCount++;
        const fileName = (doc?.props?.file_name)?doc.props.file_name : (doc?.props?.data_file_name) ? doc?.props?.data_file_name : null;
        doc["dataFileLocation"] = getDataFileUrl(doc?.submissionID, fileName);
        if (doc["dataFileLocation"]) {
            bulkOps.push({
                updateOne: {
                    filter: { _id: doc._id },
                    update: { $set: {"dataFileLocation": doc["dataFileLocation"] } }
                }
            });
            updatedCount++;
        }
        else {
            print(`No data file location is restored because missing one or more related property.`);
        }
    });

    if (bulkOps.length > 0) {
        db.release.bulkWrite(bulkOps);
    }

    console.log(`Matched Records: ${matchedCount}`);
    console.log(`Updated Records: ${updatedCount}`);
    console.log(`${new Date()} -> Restored data field: "dataFileLocation"`);
    print("----------------------");
    print("\n");
}
  
// Restore version in applications
restoreFileLocation();
