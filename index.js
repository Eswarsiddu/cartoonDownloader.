import { Worker } from "worker_threads";
// import  EpisodesData  from "./sortedEpisodesData.json";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const EpisodesData = JSON.parse(
  fs.readFileSync(path.join(__dirname, "sortedEpisodesData.json"), "utf-8")
);
// Configuration
const MAX_WORKERS = 10; // Optimized for 2 vCPU CloudShell with 3.7GB RAM
const PROGRESS_FILE = "./progress.json";
const COMPLETED_FILE = "./completedEpisodes.json";
const CURRENT_PROGRESS_FILE = "./currentprogress.txt";

let progressLog = [];

// Initialize progress and completed episodes files
function initializeFiles() {
  if (!fs.existsSync(COMPLETED_FILE)) {
    fs.writeFileSync(COMPLETED_FILE, JSON.stringify([], null, 2));
  }

  if (!fs.existsSync(PROGRESS_FILE)) {
    const initialProgress = {
      totalEpisodes: EpisodesData.length,
      processedCount: 0,
      failedCount: 0,
      startTime: new Date().toISOString(),
      lastUpdated: new Date().toISOString(),
      activeWorkers: 0,
      completedEpisodes: [],
    };
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(initialProgress, null, 2));
  }
}

// Load progress
function loadProgress() {
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf-8"));
  } catch (error) {
    console.error("Error loading progress:", error);
    return null;
  }
}

// Write progress log to file
function writeProgressLog(message) {
  progressLog.push(`[${new Date().toLocaleTimeString()}] ${message}`);
  try {
    fs.writeFileSync(CURRENT_PROGRESS_FILE, progressLog.join("\n"), "utf8");
  } catch (error) {
    // Silently handle write errors
  }
}

// Save progress
function saveProgress(progress) {
  try {
    progress.lastUpdated = new Date().toISOString();
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
  } catch (error) {
    // Silently handle save errors
  }
}

// Add completed episode
function addCompletedEpisode(episodeData) {
  try {
    const completedEpisodes = JSON.parse(
      fs.readFileSync(COMPLETED_FILE, "utf-8")
    );
    completedEpisodes.push(episodeData.index);
    fs.writeFileSync(
      COMPLETED_FILE,
      JSON.stringify(completedEpisodes, null, 2)
    );

    // Update progress
    const progress = loadProgress();
    if (progress) {
      progress.processedCount++;
      progress.completedEpisodes = completedEpisodes;
      saveProgress(progress);
    }
  } catch (error) {
    // Silently handle errors
  }
}

// Get episodes that haven't been processed yet
function getUnprocessedEpisodes() {
  try {
    const completedEpisodes = JSON.parse(
      fs.readFileSync(COMPLETED_FILE, "utf-8")
    );
    return EpisodesData.filter(
      (episode) => !completedEpisodes.includes(episode.index)
    );
  } catch (error) {
    return EpisodesData;
  }
}

// Create and manage worker
function createWorker(episode, workerId) {
  return new Promise((resolve, reject) => {
    const workerPath = path.join(__dirname, "Worker.js");
    const worker = new Worker(workerPath, {
      workerData: { episode, workerId },
    });

    // Update active workers count
    const progress = loadProgress();
    if (progress) {
      progress.activeWorkers++;
      saveProgress(progress);
    }

    worker.on("message", (result) => {
      if (result.type === "progress") {
        writeProgressLog(result.message);
      } else if (result.success) {
        writeProgressLog(
          `✅ Worker ${workerId}: Completed ${result.data.title} (Episode ${result.data.globalEpisodeNumber})`
        );
        addCompletedEpisode(result.data);
        resolve(result.data);
      } else {
        writeProgressLog(
          `❌ Worker ${workerId}: Failed ${episode.title} - ${result.error}`
        );

        // Update failed count
        const progress = loadProgress();
        if (progress) {
          progress.failedCount++;
          saveProgress(progress);
        }

        reject(new Error(result.error));
      }
    });

    worker.on("error", (error) => {
      writeProgressLog(`💥 Worker ${workerId} error: ${error.message}`);

      // Update failed count
      const progress = loadProgress();
      if (progress) {
        progress.failedCount++;
        saveProgress(progress);
      }

      reject(error);
    });

    worker.on("exit", (code) => {
      // Update active workers count
      const progress = loadProgress();
      if (progress) {
        progress.activeWorkers--;
        saveProgress(progress);
      }
    });
  });
}

// Process episodes in parallel batches
async function processEpisodesInParallel() {
  console.log("🚀 Starting parallel episode processing...");

  const unprocessedEpisodes = getUnprocessedEpisodes();

  if (unprocessedEpisodes.length === 0) {
    writeProgressLog("✅ All episodes have been processed!");
    return;
  }

  writeProgressLog(
    `📊 Found ${unprocessedEpisodes.length} unprocessed episodes`
  );
  writeProgressLog(`👥 Using ${MAX_WORKERS} parallel workers`);
  writeProgressLog(""); // Empty line for readability

  let currentIndex = 0;
  const workers = [];

  // Function to process next episode
  const processNext = async (workerId) => {
    while (currentIndex < unprocessedEpisodes.length) {
      const episodeIndex = currentIndex++;
      const episode = unprocessedEpisodes[episodeIndex];

      try {
        writeProgressLog(
          `🔄 Worker ${workerId}: Starting ${episode.title} (${
            episodeIndex + 1
          }/${unprocessedEpisodes.length})`
        );
        await createWorker(episode, workerId);
      } catch (error) {
        // Continue with next episode even if this one failed
      }
    }
  };

  // Start all workers
  for (let i = 0; i < MAX_WORKERS; i++) {
    workers.push(processNext(i + 1));
  }

  // Wait for all workers to complete
  try {
    await Promise.allSettled(workers);

    const finalProgress = loadProgress();
    if (finalProgress) {
      finalProgress.endTime = new Date().toISOString();
      finalProgress.activeWorkers = 0;
      saveProgress(finalProgress);

      writeProgressLog("");
      writeProgressLog("🎉 Processing completed!");
      writeProgressLog(`📊 Final Statistics:`);
      writeProgressLog(`   Total episodes: ${finalProgress.totalEpisodes}`);
      writeProgressLog(
        `   Successfully processed: ${finalProgress.processedCount}`
      );
      writeProgressLog(`   Failed: ${finalProgress.failedCount}`);
      writeProgressLog(
        `   Remaining: ${
          finalProgress.totalEpisodes - finalProgress.processedCount
        }`
      );

      if (finalProgress.startTime && finalProgress.endTime) {
        const duration =
          new Date(finalProgress.endTime) - new Date(finalProgress.startTime);
        const minutes = Math.floor(duration / 60000);
        const seconds = Math.floor((duration % 60000) / 1000);
        writeProgressLog(`   Total time: ${minutes}m ${seconds}s`);
      }
    }
  } catch (error) {
    writeProgressLog(`💥 Error in parallel processing: ${error.message}`);
  }
}

// Initialize and start processing
(async () => {
  try {
    initializeFiles();
    await processEpisodesInParallel();
  } catch (error) {
    writeProgressLog(`💥 Fatal error: ${error.message}`);
    process.exit(1);
  }
})();
