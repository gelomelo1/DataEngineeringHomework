import express from "express";
import fs from "fs";
import path from "path";

const app = express();
const PORT = 3000;

// Ensure SIMDATA_FOLDER is set
const simdataFolder = process.env.SIMDATA_FOLDER;
if (!simdataFolder) {
  console.error("SIMDATA_FOLDER environment variable is not set!");
  process.exit(1);
}

const steamDataFile = path.join(simdataFolder, "steam_data.json");

// Helper function to load and parse steam_data.json
const loadSteamData = () => {
  try {
    const raw = fs.readFileSync(steamDataFile, "utf-8");
    return JSON.parse(raw);
  } catch (err) {
    console.error("Failed to read JSON:", err);
    return {};
  }
};

// GET /steam_data → all games
app.get("/steam_data", (req, res) => {
  const data = loadSteamData();
  res.json(data);
});

// GET /steam_data/:id → single game
app.get("/steam_data/:id", (req, res) => {
  const data = loadSteamData();
  const game = data[req.params.id];
  if (game) res.json(game);
  else res.status(404).json({ error: "Game not found" });
});

app.listen(PORT, () => {
  console.log(`Steam API running at http://localhost:${PORT}`);
});
