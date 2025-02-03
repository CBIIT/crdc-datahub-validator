// Use the appropriate database
// use crdc-datahub2;

// Define the function to get the code based on title and description
function getVersionByStatus(status) {
    const currentVersion = "1.0";
    const newStatusVersion = "2.0"; 
    return ["New", "In Progress", "Inquired"].includes(status) ? newStatusVersion : currentVersion;
}
  
// Function to process and update a collection (errors or warnings)
function restoreApplicationVersion() {
    const bulkOps = [];
    let matchedCount = 0;
    let updatedCount = 0;
    print("\n");
    print("----------------------");
    console.log(`${new Date()} -> Restoring data field: "version"`);
    db.applications.find({ "version": {$exists: false}, "status": {$exists: true, $ne: null} }).forEach(doc => {
    matchedCount++;
    doc["version"] = getVersionByStatus(doc["status"]);
    bulkOps.push({
        updateOne: {
            filter: { _id: doc._id },
            update: { $set: {"version": doc["version"] } }
        }
    });
    updatedCount++;

    });

    if (bulkOps.length > 0) {
        db.applications.bulkWrite(bulkOps);
    }

    console.log(`Matched Records: ${matchedCount}`);
    console.log(`Updated Records: ${updatedCount}`);
    console.log(`${new Date()} -> Restored data field: "version"`);
    print("----------------------");
    print("\n");
}
  
// Restore version in applications
restoreApplicationVersion();
