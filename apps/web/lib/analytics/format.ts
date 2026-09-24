import type { AnalyticsMetricFormat } from "./types"

export function formatAnalyticsMetric(
  value: number | null | undefined,
  format: AnalyticsMetricFormat,
  compact = false
) {
  if (value == null || !Number.isFinite(value)) return "—"

  if (format === "percent") {
    return `${new Intl.NumberFormat("en", {
      maximumFractionDigits: 1,
    }).format(value)}%`
  }

  if (format === "currency") {
    return new Intl.NumberFormat("en", {
      style: "currency",
      currency: "THB",
      notation: compact ? "compact" : "standard",
      maximumFractionDigits: compact ? 1 : 0,
    }).format(value)
  }

  return new Intl.NumberFormat("en", {
    notation: compact ? "compact" : "standard",
    maximumFractionDigits: format === "ratio" ? 2 : compact ? 1 : 2,
  }).format(value)
}

export function formatOpportunityScore(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—"
  return new Intl.NumberFormat("en", {
    style: "percent",
    maximumFractionDigits: 1,
  }).format(value)
}
