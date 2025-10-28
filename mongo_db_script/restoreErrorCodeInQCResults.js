// Use the appropriate database
// use crdc-datahub2;

// Define the function to get the code based on title and description
function getCodeByTitle(title, description = null) {
    const mappings = {
        "Missing required property": "M003",
        "Invalid integer value": "M004",
        "Invalid number value": "M005",
        "Invalid datetime value": "M006",
        "Invalid date value": "M007",
        "Invalid boolean value": "M008",
        "Invalid property definition": "M009",
        "Value not permitted": "M010",
        "Value out of range": {
            "below": "M011",
            "above": "M012"
        },
        "Relationship not specified": "M013",
        "Related node not found": "M014",
        "Identical data found": "M015",
        "Duplicate IDs": "M016",
        "Invalid Property": "M017",
        "Invalid property": "M017",
        "Updating existing data": "M018",
        "Data not found": "M019",
        "Internal error": {
            "Metadata validation": "M020",
            "Data file validation": "F011"
        },
        "Invalid node": "M021",
        "Missing ID property": "M022",
        "Invalid relationship": "M023",
        "One-to-one relationship conflict": "M024",
        "Many-to-one relationship conflict": "M025",
        "Invalid data model": "M026",
        "Invalid record/node type": "M035",
        "Incompatible consent codes": "M036",
        "Data File not found": "F001",
        "Data file not found": "F001",
        "File records not found": "F002",
        "Data file records not found": "F002",
        "Data file size mismatch": "F003",
        "Data file MD5 mismatch": "F004",
        "Duplicated file records detected": "F005",
        "Conflict file records detected": "F006",
        "Duplicated file content detected": "F007",
        "Duplicated data file content detected": "F007",
        "Orphaned file found": "F008",
        "Invalid dataRecord": "F009",
        "Invalid data file info": "F010",
        "Conflict Data found": "S001"
    };
  
    if (title in mappings) {
        const value = mappings[title];
        if (typeof value === "string") {
            return value;
        }
        if (typeof value === "object" && typeof description === "string") {
            const matchingKey = Object.keys(value).find(k => description.includes(k));
            if (matchingKey) {
                return value[matchingKey];
            }
        }
    }
    else {
        console.error(`No code found for title: ${title}`);
        return null;

    }
       
    return null;
}

// Function to process and update a collection (errors or warnings)
function processCollection(field) {
    const bulkOps = [];
    let matchedCount = 0;
    let updatedCount = 0;
    print("\n");
    print("----------------------");
    console.log(`${new Date()} -> Processing data field: ${field}`);
    db.qcResults.find({ [field]: { $ne: null }, [`${field}.title`]: { $exists: true }, [`${field}.code`]: { $exists: false } }).forEach(doc => {
        let updated = false;
        matchedCount++;

        if (Array.isArray(doc[field])) {
            doc[field].forEach(item => {
                if (!item.code && item.title) {
                    const newCode = getCodeByTitle(item.title, item.description);
                    if (newCode) {
                        item.code = newCode;
                        updated = true;
                    }
                }
            });

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
        db.qcResults.bulkWrite(bulkOps);
    }

    console.log(`Matched Records: ${matchedCount}`);
    console.log(`Updated Records: ${updatedCount}`);
    console.log(`${new Date()} -> Processed data field: ${field}`);
    print("----------------------");
    print("\n");
}

// Restore code in qcResults.errors
processCollection("errors");
// Restore code in qcResults.warnings
processCollection("warnings");