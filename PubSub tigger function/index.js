const {Storage} = require('@google-cloud/storage');
const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const storage = new Storage();
const bucketName = 'cloud_scheduler_bucket';

exports.helloPubSub = (event, context) => {
  console.log(Buffer.from(event.data, 'base64').toString());
  listFiles();
};

async function listFiles() {
    const [files] = await storage.bucket(bucketName).getFiles();
    console.log('Files:');
    files.forEach(file => {
      const file_name = file.name;
      if (file_name.includes(".csv")){
          loadCSV(file_name);
      }
      console.log(file.name);
    });
}

async function loadCSV(filename) {
         const datasetId = 'set';
        const tableId = 'set' + filename.slice(0, -4);
        
        const metadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        autodetect: true,
        location: 'EU',
        };

        const options = {
        location: 'EU',
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

