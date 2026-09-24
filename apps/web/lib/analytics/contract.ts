import { z } from "zod"

import {
  MAX_ANALYTICS_BUCKETS,
  MAX_ANALYTICS_COMPANIES,
  MAX_ANALYTICS_DISTRIBUTIONS,
  MAX_ANALYTICS_PRODUCTS,
  MAX_ANALYTICS_SERIES,
  MAX_ANALYTICS_SERIES_POINTS,
  MAX_ANALYTICS_TERRITORIES,
  type AnalyticsSnapshot,
} from "./types"

const MAX_TEXT = 8_000
const MAX_MESSAGE = 2_000
const MAX_LABEL = 500
const MAX_KEY = 512

const nullableFinite = z.number().finite().nullable().optional()
const nullableText = z.string().max(MAX_TEXT).nullable().optional()
const boundedLabel = z.string().min(1).max(MAX_LABEL)

const populationSchema = z.object({
  key: z.string().min(1).max(MAX_KEY),
  label: boundedLabel,
  count: z.number().int().nonnegative(),
  source: z.enum(["analytics", "spatial", "relationship"]),
  exact: z.boolean(),
  expiresAt: z.string().datetime({ offset: true }).nullable().optional(),
})

const filterSchema = z.object({
  dimension: z.enum([
    "tipStatus",
    "province",
    "businessType",
    "incomeBand",
    "product",
    "subclass",
  ]),
  value: z.string().min(1).max(MAX_LABEL),
  label: boundedLabel,
})

const summarySchema = z.object({
  companyCount: z.number().int().nonnegative().nullable(),
  active: z.number().int().nonnegative().nullable(),
  expired: z.number().int().nonnegative().nullable(),
  prospect: z.number().int().nonnegative().nullable(),
  tipPenetration: z.number().finite().min(0).max(100).nullable(),
  totalIncome: z.number().finite().nullable(),
  medianIncome: z.number().finite().nullable(),
  totalPremium: z.number().finite().nullable(),
  totalLoss: z.number().finite().nullable(),
  lossRatio: z.number().finite().nullable(),
  averageOpportunityScore: z.number().finite().min(0).max(1).nullable(),
})

const companySchema = z.object({
  id: z.string().min(1).max(64),
  name: boundedLabel,
  province: nullableText,
  district: nullableText,
  businessType: nullableText,
  incomeBand: nullableText,
  tipStatus: z.enum(["active", "expired", "prospect"]).nullable().optional(),
  totalIncome: nullableFinite,
  netProfit: nullableFinite,
  premium: nullableFinite,
  loss: nullableFinite,
  lossRatio: nullableFinite,
  opportunityScore: z.number().finite().min(0).max(1).nullable().optional(),
  branchDistanceKm: z.number().finite().nonnegative().nullable().optional(),
  productCount: z.number().int().nonnegative().nullable().optional(),
  products: z.array(z.string().min(1).max(MAX_LABEL)).max(64),
  subclasses: z.array(z.string().min(1).max(MAX_LABEL)).max(128),
})

const bucketSchema = z.object({
  id: z.string().min(1).max(128),
  label: boundedLabel,
  count: z.number().int().nonnegative(),
  value: nullableFinite,
  share: z.number().finite().min(0).max(100).nullable().optional(),
  filter: filterSchema.nullable().optional(),
  population: populationSchema.nullable().optional(),
})

const distributionSchema = z.object({
  id: z.string().min(1).max(128),
  label: boundedLabel,
  format: z.enum(["currency", "number", "percent", "ratio"]),
  buckets: z.array(bucketSchema).max(MAX_ANALYTICS_BUCKETS),
})

const productSchema = z.object({
  id: z.string().min(1).max(192),
  product: boundedLabel,
  subclass: nullableText,
  companyCount: z.number().int().nonnegative(),
  opportunityCount: z.number().int().nonnegative(),
  sumInsured: nullableFinite,
  premium: nullableFinite,
  loss: nullableFinite,
  lossRatio: nullableFinite,
})

const territorySchema = z.object({
  id: z.string().min(1).max(192),
  label: boundedLabel,
  kind: z.enum(["branch", "province", "district"]),
  companyCount: z.number().int().nonnegative(),
  prospectCount: z.number().int().nonnegative(),
  opportunityScore: z.number().finite().min(0).max(1).nullable().optional(),
  premium: nullableFinite,
  lossRatio: nullableFinite,
  population: populationSchema.nullable().optional(),
})

const seriesSchema = z.object({
  id: z.string().min(1).max(128),
  label: boundedLabel,
  format: z.enum(["currency", "number", "percent", "ratio"]),
  points: z
    .array(
      z.object({
        period: z.string().min(1).max(32),
        value: z.number().finite().nullable(),
      })
    )
    .max(MAX_ANALYTICS_SERIES_POINTS),
})

const snapshotSchema = z
  .object({
    availability: z.enum(["ready", "partial", "unavailable"]),
    message: z.string().max(MAX_MESSAGE).optional(),
    generatedAt: z.string().max(64).nullable().optional(),
    population: populationSchema.nullable(),
    capabilities: z.object({
      populationAnalytics: z.boolean(),
      opportunity: z.boolean(),
      productPerformance: z.boolean(),
      territory: z.boolean(),
      localCrossFilter: z.boolean(),
    }),
    summary: summarySchema,
    distributions: z.array(distributionSchema).max(MAX_ANALYTICS_DISTRIBUTIONS),
    companies: z.array(companySchema).max(MAX_ANALYTICS_COMPANIES),
    products: z.array(productSchema).max(MAX_ANALYTICS_PRODUCTS),
    territories: z.array(territorySchema).max(MAX_ANALYTICS_TERRITORIES),
    series: z.array(seriesSchema).max(MAX_ANALYTICS_SERIES),
  })
  .superRefine((snapshot, context) => {
    validateUnique(snapshot.companies, "companies", context)
    validateUnique(snapshot.products, "products", context)
    validateUnique(snapshot.territories, "territories", context)
    validateUnique(snapshot.series, "series", context)
    validateUnique(snapshot.distributions, "distributions", context)

    for (const [index, distribution] of snapshot.distributions.entries()) {
      const ids = new Set(distribution.buckets.map((bucket) => bucket.id))
      if (ids.size !== distribution.buckets.length) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["distributions", index, "buckets"],
          message: "Analytics distribution contains duplicate bucket IDs",
        })
      }
    }
  })

function validateUnique(
  rows: Array<{ id: string }>,
  path: string,
  context: z.RefinementCtx
) {
  const ids = new Set(rows.map((row) => row.id))
  if (ids.size !== rows.length) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      path: [path],
      message: `Analytics response contains duplicate ${path} IDs`,
    })
  }
}

export function parseAnalyticsSnapshot(input: unknown): AnalyticsSnapshot {
  return snapshotSchema.parse(input) as AnalyticsSnapshot
}
