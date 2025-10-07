import { downloadFile } from "./FileDownloader.js";
import { uploadToS3 } from "./S3Uploader.js";
import { parentPort, workerData } from "worker_threads";
import fs from "fs";

// Configuration
const bucketName = process.env.S3_BUCKET_NAME || "siddu-eswar-jellyfin-media-data";

// Clean up temp files
function cleanupTempFile(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  } catch (error) {
    // Silently handle cleanup errors
  }
}

// Process a single episode
async function processEpisode(episode, workerId) {
  const { title, downloadUrl, s3Path, index, globalEpisodeNumber, ...rest } =
    episode;

  // Create unique temp file path using worker ID and timestamp to avoid conflicts
  const tempFilePath = `/home/temp_files/temp_worker${workerId}_${index}_${Date.now()}.mp4`;

  try {
    // Send progress update to parent
    if (parentPort) {
      parentPort.postMessage({
        type: "progress",
        message: `🔄 Worker ${workerId}: Starting download for ${title} (Episode ${globalEpisodeNumber})`,
      });
    }

    // Step 1: Download file
    await downloadFile(downloadUrl, tempFilePath);
    if (parentPort) {
      parentPort.postMessage({
        type: "progress",
        message: `📥 Worker ${workerId}: Download completed for ${title}`,
      });
    }

    // Step 2: Upload to S3
    if (parentPort) {
      parentPort.postMessage({
        type: "progress",
        message: `☁️ Worker ${workerId}: Starting S3 upload for ${title}`,
      });
    }
    await uploadToS3(tempFilePath, bucketName, s3Path);

    // Step 3: Clean up temp file
    cleanupTempFile(tempFilePath);

    return {
      title,
      s3Path,
      globalEpisodeNumber,
      index,
      downloadUrl,
      workerId,
      processedAt: new Date().toISOString(),
      ...rest,
    };
  } catch (error) {
    // Clean up temp file even if there's an error
    cleanupTempFile(tempFilePath);
    throw error;
  }
}

// Main worker execution
(async () => {
  if (!parentPort) {
    console.error("This script must be run as a worker thread");
    process.exit(1);
  }

  const { episode, workerId } = workerData;

  try {
    const result = await processEpisode(episode, workerId);

    // Send success message to parent
    parentPort.postMessage({
      success: true,
      data: result,
    });
  } catch (error) {
    // Send error message to parent
    parentPort.postMessage({
      success: false,
      error: error.message,
      episode: episode,
    });
  }
})();

// Handle cleanup on worker termination
process.on("SIGTERM", () => {
  process.exit(0);
});

process.on("SIGINT", () => {
  process.exit(0);
});

// Export for potential direct usage (though mainly used as worker thread)
export async function processFile(episode) {
  return await processEpisode(episode, "direct");
}
