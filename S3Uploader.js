import {
  S3Client,
  PutObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";

// Initialize S3 client
const s3Client = new S3Client({
  region: process.env.AWS_REGION || "ap-south-2",
});

// Check if file exists in S3
async function fileExistsInS3(bucketName, key) {
  try {
    await s3Client.send(
      new HeadObjectCommand({ Bucket: bucketName, Key: key })
    );
    return true;
  } catch (error) {
    if (error.name === "NotFound" || error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    throw error;
  }
}

// Get content type from file extension
function getContentType(fileName) {
  const ext = path.extname(fileName).toLowerCase();
  const types = {
    ".mp4": "video/mp4",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
    ".mkv": "video/x-matroska",
    ".webm": "video/webm",
  };
  return types[ext] || "application/octet-stream";
}

// Simple S3 upload function
export async function uploadToS3(filePath, bucketName, s3Path) {
  try {
    // Check if file already exists
    const exists = await fileExistsInS3(bucketName, s3Path);
    if (exists) {
      return {
        success: true,
        skipped: true,
        message: `File already exists: ${s3Path}`,
      };
    }

    // Read file and upload
    const fileBuffer = fs.readFileSync(filePath);
    const fileName = path.basename(filePath);

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: s3Path,
      Body: fileBuffer,
      ContentType: getContentType(fileName),
      StorageClass: "GLACIER_IR",
      Metadata: {
        originalFileName: fileName,
        uploadedAt: new Date().toISOString(),
      },
    });

    const result = await s3Client.send(command);

    return {
      success: true,
      skipped: false,
      message: `Successfully uploaded: ${s3Path}`,
      etag: result.ETag,
    };
  } catch (error) {
    throw new Error(`S3 upload failed: ${error.message}`);
  }
}

// Test S3 connection
export async function testS3Connection(bucketName) {
  try {
    await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucketName,
        Key: "test-connection-key-that-does-not-exist",
      })
    );
    return true;
  } catch (error) {
    if (error.name === "NotFound" || error.$metadata?.httpStatusCode === 404) {
      return true; // Bucket exists, key doesn't (expected)
    }
    if (error.name === "NoSuchBucket") {
      throw new Error(`Bucket "${bucketName}" does not exist`);
    }
    if (error.name === "AccessDenied") {
      throw new Error("Access denied. Check AWS credentials and permissions");
    }
    throw error;
  }
}
