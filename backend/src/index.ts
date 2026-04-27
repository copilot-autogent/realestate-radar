import express from "express";
import { createApiRouter } from "./api/routes.js";

const app = express();
const port = Number(process.env.PORT ?? 3001);

app.use(express.json());

// CORS for frontend dev server
app.use((_req, res, next) => {
  res.header("Access-Control-Allow-Origin", process.env.CORS_ORIGIN ?? "http://localhost:4321");
  res.header("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  next();
});

app.use("/api", createApiRouter());

app.listen(port, () => {
  console.log(`🏠 Real Price Radar API running on http://localhost:${port}`);
  console.log(`   Health: http://localhost:${port}/api/health`);
});
