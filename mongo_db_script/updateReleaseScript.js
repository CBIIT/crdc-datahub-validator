db.release.aggregate([
    {
        // Lookup to join submissions collection based on submissionID
        $lookup: {
            from: "submissions",
            localField: "submissionID",
            foreignField: "_id",
            as: "submission_info"
        }
    },
    {
        // Unwind to access the submission_info document as a single object
        $unwind: "$submission_info"
    },
    {
        // Set the studyID in dataRecords based on the studyID from the submissions
        $set: {
            "studyID": "$submission_info.studyID"
        }
    },
    {
        // Merge the updates back into the release collection
        $merge: {
            into: "release",
            whenMatched: "merge",
            whenNotMatched: "discard"
        }
    }
]);

db.release.updateMany(
    {},
    { $unset: { "submission_info": null } }
);


db.release.updateMany({"nodeType": "program"},
 {"$set": {"entityType": "Program"}}
);

db.release.updateMany({"nodeType": "study"},
    {"$set": {"entityType": "Study"}}
);

db.release.updateMany({"nodeType": {"$in": [ "participant", "subject", "case"]}
},
    {"$set": {"entityType": "Participant"}}
);

db.release.updateMany({"nodeType": {"$in": [ "sample", "specimen"]}
},
    {"$set": {"entityType": "Sample"}}
);

db.release.updateMany({"nodeType": {"$in": [ "principal_investigator", "study_personnel"]}
},
    {"$set": {"entityType": "Principal Investigator"}}
);

db.release.updateMany({"nodeType": {"$in": [ "file", "data_file", "clinical_measure_file", "cytogenomic_file", "radiology_file", "methylation_array_file", "sequencing_file"]}
}, {"$set": {"entityType": "File"}}
);


