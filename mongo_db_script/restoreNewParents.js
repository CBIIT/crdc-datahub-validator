// Use the appropriate database
// use crdc-datahub2;

// Function to process and update a collection (errors or warnings)
function processCollection(collectionName) {
    const bulkOps = [];
    let matchedCount = 0;
    let updatedCount = 0;
    print("\n");
    print("----------------------");
    console.log(`${new Date()} -> Processing data field: ${field}`);
    db[collectionName].find({ "parents": { $ne: null }}).forEach(doc => {
        let updated = false;
        let newParents = [];
        matchedCount++;
        const parent_types = new Set(doc["parents"].map(parent => parent.type));
        parent_types.forEach(type => {
            const sameTypeParents = doc["parents"].filter(parent => parent.type === type);
            const newParent = sameTypeParents[0];
            if (sameTypeParents.length > 1) {
                const newParentIds = sameTypeParents.map(parent => parent.id).join("|");
                newParent["newParent"] = newParentIds;
            }
            newParents.push(newParent);
            if (updated) {
                updatedCount++;
                bulkOps.push({
                    updateOne: {
                        filter: { _id: doc._id },
                        update: { $set: { [field]: doc[field] } }
                    }
                });
            }
        }
    });

    if (bulkOps.length > 0) {
        db[collectionName].bulkWrite(bulkOps);
    }

    console.log(`Matched Records: ${matchedCount}`);
    console.log(`Updated Records: ${updatedCount}`);
    console.log(`${new Date()} -> Processed collection: ${collectionName}`);
    print("----------------------");
    print("\n");
}

// Restore parents to new format
processCollection("dataRecords");
// Restore code in qcResults.warnings
processCollection("release");