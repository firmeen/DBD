import { z } from "zod"

import type { AnalyticsFilter, AnalyticsMode } from "./types"

const filterSchema = z
  .object({
    dimension: z.enum([
      "tipStatus",
      "province",
      "businessType",
      "incomeBand",
      "product",
      "subclass",
      "productSubclass",
    ]),
    value: z.string().min(1).max(500),
    label: z.string().min(1).max(500),
    product: z.string().min(1).max(500).optional(),
    subclass: z.string().min(1).max(500).optional(),
  })
  .superRefine((filter, context) => {
    if (
      filter.dimension === "productSubclass" &&
      (!filter.product || !filter.subclass)
    ) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        message: "Product/subclass snapshot filters require both values",
      })
    }
  })

const snapshotSchema = z.object({
  mode: z.enum(["population", "opportunity"]),
  filters: z.array(filterSchema).max(12),
  candidateRailOpen: z.boolean(),
})

export interface AnalyticsSurfaceSnapshot {
  mode: AnalyticsMode
  filters: AnalyticsFilter[]
  candidateRailOpen: boolean
}

export function parseAnalyticsSurfaceSnapshot(
  input: unknown,
  mode: AnalyticsMode
): AnalyticsSurfaceSnapshot {
  const parsed = snapshotSchema.safeParse(input)
  if (!parsed.success || parsed.data.mode !== mode) {
    return defaultAnalyticsSurfaceSnapshot(mode)
  }
  return parsed.data
}

export function defaultAnalyticsSurfaceSnapshot(
  mode: AnalyticsMode
): AnalyticsSurfaceSnapshot {
  return { mode, filters: [], candidateRailOpen: true }
}
