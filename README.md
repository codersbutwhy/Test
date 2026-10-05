# Test
const {
  S3Client,
  GetObjectCommand
} = require("@aws-sdk/client-s3");

const {
  DynamoDBClient
} = require("@aws-sdk/client-dynamodb");

const {
  DynamoDBDocumentClient,
  PutCommand
} = require("@aws-sdk/lib-dynamodb");

const { parse } = require("csv-parse");

// --------------------------------------------------
// Configuration
// --------------------------------------------------

const TABLE_NAME = process.env.TABLE_NAME;

// AWS SDK v3 already provides retry/backoff for
// throttling and transient AWS errors.
const dynamoClient = new DynamoDBClient({
  maxAttempts: 5,
  retryMode: "adaptive"
});

const dynamo = DynamoDBDocumentClient.from(dynamoClient);

const s3 = new S3Client({
  maxAttempts: 5,
  retryMode: "adaptive"
});

// --------------------------------------------------
// Lambda Handler
// --------------------------------------------------

exports.handler = async (event) => {

  const bucket = event.bucket;
  const key = event.key;

  if (!bucket || !key) {
    throw new Error("bucket and key are required");
  }

  console.log("Starting incremental load", {
    bucket,
    key,
    table: TABLE_NAME
  });

  const summary = {
    totalRecords: 0,
    inserted: 0,
    alreadyExists: 0,
    invalid: 0,
    failed: 0
  };

  const failedRecords = [];

  try {

    // ----------------------------------------------
    // Get CSV from S3
    // ----------------------------------------------

    const response = await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key
      })
    );

    // ----------------------------------------------
    // Stream CSV
    // ----------------------------------------------

    const parser = response.Body.pipe(
      parse({
        columns: true,
        skip_empty_lines: true,
        trim: true
      })
    );

    // ----------------------------------------------
    // Process each record
    // ----------------------------------------------

    for await (const record of parser) {

      summary.totalRecords++;

      const mobile = normalizeMobile(record.Mobile);

      // --------------------------------------------
      // Validate mobile
      // --------------------------------------------

      if (!mobile) {

        summary.invalid++;

        failedRecords.push({
          mobile: record.Mobile || null,
          reason: "INVALID_MOBILE"
        });

        continue;
      }

      // --------------------------------------------
      // Insert into DynamoDB
      // --------------------------------------------

      try {

        const result = await insertIfNotExists(mobile);

        if (result === "INSERTED") {
          summary.inserted++;
        }

        if (result === "EXISTS") {
          summary.alreadyExists++;
        }

      } catch (error) {

        console.error("Failed to process mobile", {
          mobile,
          errorName: error.name,
          errorMessage: error.message
        });

        summary.failed++;

        failedRecords.push({
          mobile,
          reason: error.name || "DYNAMODB_ERROR",
          message: error.message
        });
      }
    }

    // ----------------------------------------------
    // Final summary
    // ----------------------------------------------

    const result = {
      status: summary.failed > 0 ? "PARTIAL_SUCCESS" : "SUCCESS",
      bucket,
      key,
      summary
    };

    console.log("Load completed", result);

    // You can later write failedRecords to S3
    // if required.

    return {
      statusCode: summary.failed > 0 ? 207 : 200,
      body: JSON.stringify(result)
    };

  } catch (error) {

    console.error("Load failed completely", {
      errorName: error.name,
      errorMessage: error.message
    });

    throw error;
  }
};


// --------------------------------------------------
// Insert only if Mobile doesn't already exist
// --------------------------------------------------

async function insertIfNotExists(mobile) {

  try {

    await dynamo.send(
      new PutCommand({
        TableName: TABLE_NAME,

        Item: {
          Mobile: mobile
        },

        // IMPORTANT:
        // Insert only when Mobile doesn't exist.
        ConditionExpression: "attribute_not_exists(Mobile)"
      })
    );

    return "INSERTED";

  } catch (error) {

    // ----------------------------------------------
    // This is NOT an actual failure.
    // It means the mobile already exists.
    // ----------------------------------------------

    if (error.name === "ConditionalCheckFailedException") {
      return "EXISTS";
    }

    // ----------------------------------------------
    // Any other error is a real failure.
    // AWS SDK retry mechanism has already attempted
    // retryable errors according to maxAttempts.
    // ----------------------------------------------

    throw error;
  }
}


// --------------------------------------------------
// Normalize Mobile
// --------------------------------------------------

function normalizeMobile(value) {

  if (value === undefined || value === null) {
    return null;
  }

  let mobile = String(value).trim();

  if (!mobile) {
    return null;
  }

  // Remove spaces, -, brackets, etc.
  mobile = mobile.replace(/[\s\-()]/g, "");

  // Example:
  // +919876543210 -> 9876543210
  if (mobile.startsWith("+91")) {
    mobile = mobile.substring(3);
  }

  // Example:
  // 919876543210 -> 9876543210
  if (mobile.startsWith("91") && mobile.length === 12) {
    mobile = mobile.substring(2);
  }

  // Indian mobile validation
  if (!/^[6-9]\d{9}$/.test(mobile)) {
    return null;
  }

  return mobile;
}
