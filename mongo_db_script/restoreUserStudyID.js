// Use the appropriate database
// use crdc-datahub2;
  
// Function to restore user studies in a format of [{_id: studyID}]
function restoreUserStudies() {
    const bulkOps = [];
    let matchedCount = 0;
    let updatedCount = 0;
    print("\n");
    print("----------------------");
    console.log(`${new Date()} -> Restoring data field: "studies"`);
    db.users.find({"studies": {$exists: true, $ne: null}, "studies._id": {$exists: false} }).forEach(doc => {
        matchedCount++;
        doc["studies"] = doc["studies"].map(s => ({"_id": s}));
        bulkOps.push({
            updateOne: {
                filter: { _id: doc._id },
                update: { $set: {"studies": doc["studies"] } }
            }
        });
        updatedCount++;
    });

    if (bulkOps.length > 0) {
        db.users.bulkWrite(bulkOps);
    }

    console.log(`Matched Records: ${matchedCount}`);
    console.log(`Updated Records: ${updatedCount}`);
    console.log(`${new Date()} -> Restored data field: "studies"`);
    print("----------------------");
    print("\n");
}
  
// Restore studies in user
restoreUserStudies();
