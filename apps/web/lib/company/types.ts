import type { EntityRef, LensState } from "@/lib/intelligence/types"

export const COMPANY_VIEW_IDS = [
  "dossier",
  "financial",
  "position",
  "ownership",
  "history",
] as const

export type CompanyViewId = (typeof COMPANY_VIEW_IDS)[number]
export type CompanyAvailability = "ready" | "partial" | "unavailable"
export type CompanyMetricFormat = "currency" | "number" | "percent" | "ratio"

export const MAX_COMPANY_SEARCH_RESULTS = 50
export const MAX_COMPANY_LINE_ITEMS = 2_500

export interface CompanySearchResult {
  id: string
  nameTh: string
  nameEn?: string | null
  statusDescription?: string | null
  businessType?: string | null
  province?: string | null
  latestFiscalYear?: string | null
}

export interface CompanySearchSnapshot {
  availability: CompanyAvailability
  message?: string
  generatedAt?: string | null
  results: CompanySearchResult[]
}

export interface CompanyProfile {
  id: string
  jpTypeCode?: string | null
  nameTh: string
  nameEn?: string | null
  statusCode?: string | null
  statusDescription?: string | null
  typeDescription?: string | null
  businessTypeCode?: string | null
  businessTypeDescription?: string | null
  businessSizeCode?: string | null
  setupObjective?: string | null
  address?: string | null
  district?: string | null
  subdistrict?: string | null
  province?: string | null
  postalCode?: string | null
  registrationDate?: string | null
  statusChangeDate?: string | null
  registeredCapital?: number | null
  paidCapital?: number | null
  totalAssets?: number | null
  totalEquity?: number | null
  totalIncome?: number | null
  netProfit?: number | null
  latestFiscalYear?: string | null
}

export interface CompanyRatio {
  code: string
  label: string
  value: number | null
  format: Exclude<CompanyMetricFormat, "currency">
}

export interface CompanyFinancialLineItem {
  code: string
  label: string
  amount: number | null
  previousAmount: number | null
  pctChange: number | null
}

export interface CompanyFinancialYear {
  year: string
  fiscalYearEndDate?: string | null
  submittedAt?: string | null
  status?: string | null
  ratios: CompanyRatio[]
  lineItems: CompanyFinancialLineItem[]
}

export interface CompanyFinancialSeries {
  code: string
  label: string
  format: CompanyMetricFormat
  points: Array<{
    year: string
    value: number | null
  }>
}

export interface CompanyIndustryMetric {
  code: string
  label: string
  format: CompanyMetricFormat
  companyValue: number | null
  industryAverage: number | null
  industryAveragePrevious: number | null
  industryTotal: number | null
  pctChange: number | null
}

export interface CompanyIndustrySnapshot {
  fiscalYear: string
  submitFinancialCount: number | null
  profitCount: number | null
  lossCount: number | null
  metrics: CompanyIndustryMetric[]
}

export interface CompanyNation {
  code: string
  country: string
  countryEn?: string | null
  shareAmount?: number | null
  shareQuantity?: number | null
  sharePct: number | null
}

export interface CompanyNationHistory {
  fiscalYear: string
  thaiPct: number | null
  otherPct: number | null
}

export type CompanyHistoryKind =
  | "name"
  | "capital"
  | "transform"
  | "merger"
  | "submission"
  | "status"

export interface CompanyHistoryEvent {
  id: string
  kind: CompanyHistoryKind
  date?: string | null
  title: string
  detail?: string | null
  previousAmount?: number | null
  newAmount?: number | null
  relatedCompany?: EntityRef | null
}

export interface CompanyTipContext {
  status?: "active" | "expired" | "prospect" | null
  productCount?: number | null
  premium?: number | null
  loss?: number | null
  lossRatio?: number | null
}

export interface CompanyBoardSummary {
  memberCount: number | null
}

export interface CompanyCapabilities {
  financials: boolean
  industryBenchmark: boolean
  nationality: boolean
  corporateHistory: boolean
  tipContext: boolean
  boardSummary: boolean
}

export interface CompanyDetailSnapshot {
  availability: CompanyAvailability
  message?: string
  generatedAt?: string | null
  capabilities: CompanyCapabilities
  profile: CompanyProfile | null
  financial: {
    years: CompanyFinancialYear[]
    series: CompanyFinancialSeries[]
  }
  industry: CompanyIndustrySnapshot | null
  nationality: {
    current: CompanyNation[]
    history: CompanyNationHistory[]
  }
  history: CompanyHistoryEvent[]
  tipContext: CompanyTipContext | null
  boardSummary: CompanyBoardSummary | null
}

export interface CompanySearchRequest {
  operation: "search"
  query: string
  limit: number
  lens: LensState
}

export interface CompanyDetailRequest {
  operation: "detail"
  companyId: string
  lens: LensState
}

export type CompanyServingRequest = CompanySearchRequest | CompanyDetailRequest

export interface CompanySurfaceSnapshot {
  focusedCompanyId: string | null
  view: CompanyViewId
  searchTerm: string
  financialYear: string | null
}

export function companySearchResultToEntity(
  company: CompanySearchResult
): EntityRef {
  return {
    id: company.id,
    kind: "company",
    label: company.nameTh,
    secondary:
      [company.businessType, company.province].filter(Boolean).join(" · ") ||
      company.nameEn ||
      undefined,
  }
}

export function companyProfileToEntity(profile: CompanyProfile): EntityRef {
  return {
    id: profile.id,
    kind: "company",
    label: profile.nameTh,
    secondary:
      [profile.businessTypeDescription, profile.province]
        .filter(Boolean)
        .join(" · ") ||
      profile.nameEn ||
      undefined,
  }
}
