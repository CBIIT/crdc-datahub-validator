// This script updates the 'status' field in the 'release' collection:
// If 'status' is an array of strings, it sets it to the first string in the array.

const collection = db.getCollection('release');
let count = 0;

collection.find({ status: { $type: 'array' } }).forEach(function(doc) {
    if (Array.isArray(doc.status) && doc.status.length > 0) {
        collection.updateOne(
            { _id: doc._id },
            { $set: { status: doc.status[0] } }
        );
        count++;
    }
});

print('Updated ' + count + ' documents.');
