import { Router } from "express";
import { transactionsRouter } from "./transactions.js";

export function createApiRouter(): Router {
  const router = Router();

  router.get("/health", (_req, res) => {
    res.json({ status: "ok", timestamp: new Date().toISOString() });
  });

  router.use("/transactions", transactionsRouter());

  return router;
}
