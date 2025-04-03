db.dataRecords.aggregate([
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
        // Merge the updates back into the dataRecords collection
        $merge: {
            into: "dataRecords",
            whenMatched: "merge",
            whenNotMatched: "discard"
        }
    }
]);

db.dataRecords.updateMany(
    {},
    { $unset: { "submission_info": null } }
);
