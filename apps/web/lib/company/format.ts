import type { CompanyMetricFormat } from "./types"

const compactCurrency = new Intl.NumberFormat("th-TH", {
  style: "currency",
  currency: "THB",
  notation: "compact",
  maximumFractionDigits: 1,
})

const fullCurrency = new Intl.NumberFormat("th-TH", {
  style: "currency",
  currency: "THB",
  maximumFractionDigits: 0,
})

const compactNumber = new Intl.NumberFormat("en", {
  notation: "compact",
  maximumFractionDigits: 1,
})

const decimalNumber = new Intl.NumberFormat("en", {
  maximumFractionDigits: 2,
})

export function formatCompanyMetric(
  value: number | null | undefined,
  format: CompanyMetricFormat = "number",
  compact = false
) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "—"
  }

  if (format === "currency") {
    return compact ? compactCurrency.format(value) : fullCurrency.format(value)
  }

  if (format === "percent") {
    return `${decimalNumber.format(value)}%`
  }

  return compact ? compactNumber.format(value) : decimalNumber.format(value)
}

export function formatCompanyDate(value: string | null | undefined) {
  if (!value) return "—"
  const date = new Date(`${value}T00:00:00Z`)
  if (Number.isNaN(date.getTime())) return value

  return new Intl.DateTimeFormat("th-TH", {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(date)
}

export function formatSignedChange(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "—"
  }

  const prefix = value > 0 ? "+" : ""
  return `${prefix}${decimalNumber.format(value)}%`
}
