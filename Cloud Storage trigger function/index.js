const {BigQuery} = require('@google-cloud/bigquery');
const {Storage} = require('@google-cloud/storage');
const bigquery = new BigQuery();
const storage = new Storage();

exports.main = (event, context) => {
    const gcsEvent = event;
    console.log(`Processing file: ${gcsEvent.name}`);
    const bucketName = gcsEvent.bucket;
    const filename = gcsEvent.name;
    loadCSV(bucketName, filename);
};

async function loadCSV(bucketName, filename) {
        const datasetId = 'set';
        const tableId = 'set' + filename.slice(0, -4);
        
        const metadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        autodetect: true,
        location: 'US',
        };

        const options = {
        location: 'US',
        };

        const [table] = await bigquery
        .dataset(datasetId)
        .createTable(tableId, options);

        const [job] = await bigquery
            .dataset(datasetId)
            .table(tableId)
            .load(storage.bucket(bucketName).file(filename), metadata);

        const errors = job.status.errors;
        if (errors && errors.length > 0) {
            throw errors;
        }
        
                
    } 