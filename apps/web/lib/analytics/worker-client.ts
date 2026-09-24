"use client"

import { wrap } from "comlink"

import { computeAnalyticsSubset } from "./compute"
import type {
  AnalyticsCompanyRow,
  AnalyticsDerivedSubset,
  AnalyticsFilter,
} from "./types"
import type { AnalyticsWorkerApi } from "@/workers/analytics.worker"

const WORKER_THRESHOLD = 750

declare global {
  var __tipxAnalyticsWorker: Worker | undefined
  var __tipxAnalyticsWorkerProxy:
    | ReturnType<typeof wrap<AnalyticsWorkerApi>>
    | undefined
}

export async function computeAnalyticsSubsetOffMainThread(
  rows: AnalyticsCompanyRow[],
  filters: AnalyticsFilter[]
): Promise<AnalyticsDerivedSubset> {
  if (rows.length < WORKER_THRESHOLD || typeof Worker === "undefined") {
    return computeAnalyticsSubset(rows, filters)
  }

  if (!globalThis.__tipxAnalyticsWorker) {
    globalThis.__tipxAnalyticsWorker = new Worker(
      new URL("../../workers/analytics.worker.ts", import.meta.url),
      { type: "module", name: "tipx-analytics" }
    )
  }

  if (!globalThis.__tipxAnalyticsWorkerProxy) {
    globalThis.__tipxAnalyticsWorkerProxy = wrap<AnalyticsWorkerApi>(
      globalThis.__tipxAnalyticsWorker
    )
  }

  return globalThis.__tipxAnalyticsWorkerProxy.computeSubset(rows, filters)
}
