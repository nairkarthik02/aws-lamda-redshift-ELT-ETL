const { MongoClient } = require('mongodb');
const { createObjectCsvWriter } = require('csv-writer');
const AWS = require('aws-sdk');
const fs = require('fs');
const cron = require('node-cron');
const moment = require('moment');

// MongoDB parameters
const mongoUri = 'your_mongodb_uri';
const databaseName = 'your_database_name';
const collectionName = 'your_collection_name';

// AWS S3 parameters
const s3BucketName = 'your_bucket_name';

// Function to fetch data from MongoDB and write to CSV
async function mongoToCsv() {
  const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
  const currentDate = moment().format('YYYY-MM-DD');
  const csvFileName = `snapshot_${currentDate}.csv`;
  try {
    await client.connect();
    const db = client.db(databaseName);
    const collection = db.collection(collectionName);

    // Fetch data based on updatedAt field (e.g., last 24 hours)
    const oneDayAgo = new Date();
    oneDayAgo.setDate(oneDayAgo.getDate() - 1);

    const data = await collection.find({ updatedAt: { $gte: oneDayAgo } }).toArray();

    if (data.length === 0) {
      console.log('No data found in the collection for the given updatedAt criteria.');
      return null;
    }

    const csvWriter = createObjectCsvWriter({
      path: csvFileName,
      header: Object.keys(data[0]).map(key => ({ id: key, title: key }))
    });

    await csvWriter.writeRecords(data);
    console.log(`Data saved to ${csvFileName}`);
    return csvFileName;
  } finally {
    await client.close();
  }
}

// Function to upload CSV file to AWS S3
async function uploadToS3(csvFileName) {
  const s3 = new AWS.S3();

  const fileContent = fs.readFileSync(csvFileName);

  const params = {
    Bucket: s3BucketName,
    Key: `${moment().format('YYYY-MM-DD')}/${csvFileName}`,
    Body: fileContent,
    ContentType: 'text/csv'
  };

  s3.upload(params, function(err, data) {
    if (err) {
      console.error('Error uploading to S3:', err);
      return;
    }
    console.log(`Upload Successful: ${data.Location}`);
  });
}

// Main function to perform the tasks
async function main() {
  const csvFileName = await mongoToCsv();
  if (csvFileName) {
    await uploadToS3(csvFileName);
  }
}

// Schedule the script to run daily at midnight
cron.schedule('0 0 * * *', () => {
  console.log('Running cron job at midnight');
  main().catch(console.error);
});

// Run the main function immediately if needed
main().catch(console.error);
