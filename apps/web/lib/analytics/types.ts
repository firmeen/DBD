import type { EntityRef, LensState, PopulationRef } from "@/lib/intelligence/types"

export type AnalyticsMode = "population" | "opportunity"
export type AnalyticsAvailability = "ready" | "partial" | "unavailable"
export type AnalyticsMetricFormat = "currency" | "number" | "percent" | "ratio"

export type AnalyticsFilterDimension =
  | "tipStatus"
  | "province"
  | "businessType"
  | "incomeBand"
  | "product"
  | "subclass"
  | "productSubclass"

export interface AnalyticsFilter {
  dimension: AnalyticsFilterDimension
  value: string
  label: string
  product?: string
  subclass?: string
}

export const MAX_ANALYTICS_COMPANIES = 8_000
export const MAX_ANALYTICS_DISTRIBUTIONS = 24
export const MAX_ANALYTICS_BUCKETS = 256
export const MAX_ANALYTICS_PRODUCTS = 512
export const MAX_ANALYTICS_TERRITORIES = 512
export const MAX_ANALYTICS_SERIES = 32
export const MAX_ANALYTICS_SERIES_POINTS = 64
export const MAX_ANALYTICS_GLOBAL_SELECTION = 500

export interface AnalyticsCapabilities {
  populationAnalytics: boolean
  opportunity: boolean
  productPerformance: boolean
  territory: boolean
  localCrossFilter: boolean
}

export interface AnalyticsSummary {
  companyCount: number | null
  active: number | null
  expired: number | null
  prospect: number | null
  tipPenetration: number | null
  totalIncome: number | null
  medianIncome: number | null
  totalPremium: number | null
  totalLoss: number | null
  lossRatio: number | null
  averageOpportunityScore: number | null
}

export interface AnalyticsProductSubclassRef {
  product: string
  subclass: string
}

export interface AnalyticsCompanyRow {
  id: string
  name: string
  province?: string | null
  district?: string | null
  businessType?: string | null
  incomeBand?: string | null
  tipStatus?: "active" | "expired" | "prospect" | null
  totalIncome?: number | null
  netProfit?: number | null
  premium?: number | null
  loss?: number | null
  lossRatio?: number | null
  opportunityScore?: number | null
  branchDistanceKm?: number | null
  productCount?: number | null
  products: string[]
  subclasses: string[]
  productSubclasses: AnalyticsProductSubclassRef[]
}

export interface AnalyticsBucket {
  id: string
  label: string
  count: number
  value?: number | null
  share?: number | null
  filter?: AnalyticsFilter | null
  population?: PopulationRef | null
}

export interface AnalyticsDistribution {
  id: string
  label: string
  format: AnalyticsMetricFormat
  buckets: AnalyticsBucket[]
}

export interface AnalyticsProductPerformance {
  id: string
  product: string
  subclass?: string | null
  companyCount: number
  opportunityCount: number
  sumInsured?: number | null
  premium?: number | null
  loss?: number | null
  lossRatio?: number | null
}

export interface AnalyticsTerritory {
  id: string
  label: string
  kind: "branch" | "province" | "district"
  companyCount: number
  prospectCount: number
  opportunityScore?: number | null
  premium?: number | null
  lossRatio?: number | null
  population?: PopulationRef | null
}

export interface AnalyticsSeriesPoint {
  period: string
  value: number | null
}

export interface AnalyticsSeries {
  id: string
  label: string
  format: AnalyticsMetricFormat
  points: AnalyticsSeriesPoint[]
}

export interface AnalyticsSnapshot {
  availability: AnalyticsAvailability
  message?: string
  generatedAt?: string | null
  population: PopulationRef | null
  capabilities: AnalyticsCapabilities
  summary: AnalyticsSummary
  distributions: AnalyticsDistribution[]
  companies: AnalyticsCompanyRow[]
  products: AnalyticsProductPerformance[]
  territories: AnalyticsTerritory[]
  series: AnalyticsSeries[]
}

export interface AnalyticsServingRequest {
  operation: AnalyticsMode
  lens: LensState
  focus: EntityRef | null
  selection: EntityRef[]
  compare: EntityRef[]
  population: PopulationRef | null
  maxCompanies: number
}

export interface AnalyticsDerivedDistribution {
  dimension: Extract<
    AnalyticsFilterDimension,
    "tipStatus" | "province" | "businessType" | "incomeBand"
  >
  buckets: Array<{
    value: string
    label: string
    count: number
  }>
}

export interface AnalyticsDerivedSubset {
  companyIds: string[]
  summary: AnalyticsSummary
  distributions: AnalyticsDerivedDistribution[]
}
