import axios from "axios";
import fs from "fs";

export async function downloadFile(url, filePath) {
  try {
    const response = await axios({
      method: "GET",
      url: url,
      responseType: "stream",
      timeout: 0, // No timeout - allow very long downloads
    });

    const writer = fs.createWriteStream(filePath);
    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on("finish", resolve);
      writer.on("error", reject);
    });
  } catch (error) {
    throw new Error(`Download failed: ${error.message}`);
  }
}
