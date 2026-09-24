import { parseAnalyticsSnapshot } from "./contract"
import {
  MAX_ANALYTICS_COMPANIES,
  type AnalyticsMode,
  type AnalyticsSnapshot,
  type AnalyticsServingRequest,
} from "./types"
import type {
  EntityRef,
  LensState,
  PopulationRef,
} from "@/lib/intelligence/types"

const EMPTY_SUMMARY = {
  companyCount: null,
  active: null,
  expired: null,
  prospect: null,
  tipPenetration: null,
  totalIncome: null,
  medianIncome: null,
  totalPremium: null,
  totalLoss: null,
  lossRatio: null,
  averageOpportunityScore: null,
} as const

const NO_CAPABILITIES = {
  populationAnalytics: false,
  opportunity: false,
  productPerformance: false,
  territory: false,
  localCrossFilter: false,
} as const

export function getAnalyticsEndpoint() {
  const value = process.env.NEXT_PUBLIC_TIPX_ANALYTICS_ENDPOINT?.trim()
  return value || null
}

export function analyticsEndpointConfigured() {
  return getAnalyticsEndpoint() !== null
}

export async function fetchAnalyticsSnapshot(
  mode: AnalyticsMode,
  context: {
    lens: LensState
    focus: EntityRef | null
    selection: EntityRef[]
    compare: EntityRef[]
    population: PopulationRef | null
  },
  signal?: AbortSignal
): Promise<AnalyticsSnapshot> {
  const endpoint = getAnalyticsEndpoint()

  if (!endpoint) {
    return {
      availability: "unavailable",
      message:
        "Center Database analytics serving is not configured. TIPX preserves the active context without inventing population or opportunity values.",
      generatedAt: null,
      population: context.population,
      capabilities: NO_CAPABILITIES,
      summary: EMPTY_SUMMARY,
      distributions: [],
      companies: [],
      products: [],
      territories: [],
      series: [],
    }
  }

  const request: AnalyticsServingRequest = {
    operation: mode,
    lens: context.lens,
    focus: context.focus,
    selection: context.selection,
    compare: context.compare,
    population: context.population,
    maxCompanies: MAX_ANALYTICS_COMPANIES,
  }

  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify(request),
    cache: "no-store",
    signal,
  })

  if (!response.ok) {
    throw new Error(
      `Analytics serving request failed with HTTP ${response.status}`
    )
  }

  return parseAnalyticsSnapshot(await response.json())
}
