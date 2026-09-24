import { z } from "zod"

import {
  MAX_COMPANY_LINE_ITEMS,
  MAX_COMPANY_SEARCH_RESULTS,
  type CompanyDetailSnapshot,
  type CompanySearchSnapshot,
} from "./types"

const nullableFiniteNumber = z.number().finite().nullable()
const optionalNullableFiniteNumber = nullableFiniteNumber.optional()
const nullableText = z.string().nullable().optional()
const dateText = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .nullable()
  .optional()

const searchResultSchema = z.object({
  id: z.string().min(1).max(32),
  nameTh: z.string().min(1).max(500),
  nameEn: nullableText,
  statusDescription: nullableText,
  businessType: nullableText,
  province: nullableText,
  latestFiscalYear: nullableText,
})

const searchSnapshotSchema = z.object({
  availability: z.enum(["ready", "partial", "unavailable"]),
  message: z.string().optional(),
  generatedAt: nullableText,
  results: z.array(searchResultSchema).max(MAX_COMPANY_SEARCH_RESULTS),
})

const profileSchema = z.object({
  id: z.string().min(1).max(32),
  jpTypeCode: nullableText,
  nameTh: z.string().min(1).max(500),
  nameEn: nullableText,
  statusCode: nullableText,
  statusDescription: nullableText,
  typeDescription: nullableText,
  businessTypeCode: nullableText,
  businessTypeDescription: nullableText,
  businessSizeCode: nullableText,
  setupObjective: nullableText,
  address: nullableText,
  district: nullableText,
  subdistrict: nullableText,
  province: nullableText,
  postalCode: nullableText,
  registrationDate: dateText,
  statusChangeDate: dateText,
  registeredCapital: optionalNullableFiniteNumber,
  paidCapital: optionalNullableFiniteNumber,
  totalAssets: optionalNullableFiniteNumber,
  totalEquity: optionalNullableFiniteNumber,
  totalIncome: optionalNullableFiniteNumber,
  netProfit: optionalNullableFiniteNumber,
  latestFiscalYear: nullableText,
})

const ratioSchema = z.object({
  code: z.string().min(1).max(64),
  label: z.string().min(1).max(191),
  value: nullableFiniteNumber,
  format: z.enum(["number", "percent", "ratio"]),
})

const lineItemSchema = z.object({
  code: z.string().min(1).max(64),
  label: z.string().min(1).max(191),
  amount: nullableFiniteNumber,
  previousAmount: nullableFiniteNumber,
  pctChange: nullableFiniteNumber,
})

const financialYearSchema = z.object({
  year: z.string().min(1).max(8),
  fiscalYearEndDate: dateText,
  submittedAt: dateText,
  status: nullableText,
  ratios: z.array(ratioSchema).max(128),
  lineItems: z.array(lineItemSchema).max(MAX_COMPANY_LINE_ITEMS),
})

const seriesSchema = z.object({
  code: z.string().min(1).max(64),
  label: z.string().min(1).max(191),
  format: z.enum(["currency", "number", "percent", "ratio"]),
  points: z
    .array(
      z.object({
        year: z.string().min(1).max(8),
        value: nullableFiniteNumber,
      })
    )
    .max(32),
})

const industryMetricSchema = z.object({
  code: z.string().min(1).max(64),
  label: z.string().min(1).max(191),
  format: z.enum(["currency", "number", "percent", "ratio"]),
  companyValue: nullableFiniteNumber,
  industryAverage: nullableFiniteNumber,
  industryAveragePrevious: nullableFiniteNumber,
  industryTotal: nullableFiniteNumber,
  pctChange: nullableFiniteNumber,
})

const industrySchema = z.object({
  fiscalYear: z.string().min(1).max(8),
  submitFinancialCount: z.number().int().nonnegative().nullable(),
  profitCount: z.number().int().nonnegative().nullable(),
  lossCount: z.number().int().nonnegative().nullable(),
  metrics: z.array(industryMetricSchema).max(256),
})

const nationSchema = z.object({
  code: z.string().min(1).max(16),
  country: z.string().min(1).max(191),
  countryEn: nullableText,
  shareAmount: optionalNullableFiniteNumber,
  shareQuantity: optionalNullableFiniteNumber,
  sharePct: z.number().finite().min(0).max(100).nullable(),
})

const nationHistorySchema = z.object({
  fiscalYear: z.string().min(1).max(8),
  thaiPct: z.number().finite().min(0).max(100).nullable(),
  otherPct: z.number().finite().min(0).max(100).nullable(),
})

const relatedCompanySchema = z
  .object({
    id: z.string().min(1).max(32),
    kind: z.literal("company"),
    label: z.string().min(1).max(500),
    secondary: z.string().optional(),
  })
  .nullable()
  .optional()

const historySchema = z.object({
  id: z.string().min(1).max(128),
  kind: z.enum([
    "name",
    "capital",
    "transform",
    "merger",
    "submission",
    "status",
  ]),
  date: dateText,
  title: z.string().min(1).max(500),
  detail: nullableText,
  previousAmount: optionalNullableFiniteNumber,
  newAmount: optionalNullableFiniteNumber,
  relatedCompany: relatedCompanySchema,
})

const tipContextSchema = z.object({
  status: z.enum(["active", "expired", "prospect"]).nullable().optional(),
  productCount: z.number().int().nonnegative().nullable().optional(),
  premium: optionalNullableFiniteNumber,
  loss: optionalNullableFiniteNumber,
  lossRatio: optionalNullableFiniteNumber,
})

const boardSummarySchema = z.object({
  memberCount: z.number().int().nonnegative().nullable(),
})

const detailSnapshotSchema = z.object({
  availability: z.enum(["ready", "partial", "unavailable"]),
  message: z.string().optional(),
  generatedAt: nullableText,
  capabilities: z.object({
    financials: z.boolean(),
    industryBenchmark: z.boolean(),
    nationality: z.boolean(),
    corporateHistory: z.boolean(),
    tipContext: z.boolean(),
    boardSummary: z.boolean(),
  }),
  profile: profileSchema.nullable(),
  financial: z.object({
    years: z.array(financialYearSchema).max(32),
    series: z.array(seriesSchema).max(32),
  }),
  industry: industrySchema.nullable(),
  nationality: z.object({
    current: z.array(nationSchema).max(128),
    history: z.array(nationHistorySchema).max(32),
  }),
  history: z.array(historySchema).max(512),
  tipContext: tipContextSchema.nullable(),
  boardSummary: boardSummarySchema.nullable(),
})

export function parseCompanySearchSnapshot(
  input: unknown
): CompanySearchSnapshot {
  return searchSnapshotSchema.parse(input) as CompanySearchSnapshot
}

export function parseCompanyDetailSnapshot(
  input: unknown
): CompanyDetailSnapshot {
  return detailSnapshotSchema.parse(input) as CompanyDetailSnapshot
}
