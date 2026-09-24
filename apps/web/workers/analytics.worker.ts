import { expose } from "comlink"

import { computeAnalyticsSubset } from "@/lib/analytics/compute"
import type {
  AnalyticsCompanyRow,
  AnalyticsDerivedSubset,
  AnalyticsFilter,
} from "@/lib/analytics/types"

export interface AnalyticsWorkerApi {
  computeSubset(
    rows: AnalyticsCompanyRow[],
    filters: AnalyticsFilter[]
  ): AnalyticsDerivedSubset
}

const api: AnalyticsWorkerApi = {
  computeSubset: computeAnalyticsSubset,
}

expose(api)
