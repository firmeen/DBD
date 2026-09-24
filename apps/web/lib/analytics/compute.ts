import type {
  AnalyticsCompanyRow,
  AnalyticsDerivedDistribution,
  AnalyticsDerivedSubset,
  AnalyticsFilter,
  AnalyticsSummary,
} from "./types"

export function computeAnalyticsSubset(
  rows: AnalyticsCompanyRow[],
  filters: AnalyticsFilter[]
): AnalyticsDerivedSubset {
  const filtered = rows.filter((row) =>
    filters.every((filter) => rowMatchesFilter(row, filter))
  )

  return {
    companyIds: filtered.map((row) => row.id),
    summary: summarize(filtered),
    distributions: [
      distribution(filtered, "tipStatus"),
      distribution(filtered, "province"),
      distribution(filtered, "businessType"),
      distribution(filtered, "incomeBand"),
    ],
  }
}

function rowMatchesFilter(row: AnalyticsCompanyRow, filter: AnalyticsFilter) {
  switch (filter.dimension) {
    case "tipStatus":
      return row.tipStatus === filter.value
    case "province":
      return row.province === filter.value
    case "businessType":
      return row.businessType === filter.value
    case "incomeBand":
      return row.incomeBand === filter.value
    case "product":
      return row.products.includes(filter.value)
    case "subclass":
      return row.subclasses.includes(filter.value)
    case "productSubclass":
      return (
        filter.product != null &&
        filter.subclass != null &&
        row.productSubclasses.some(
          (pair) =>
            pair.product === filter.product &&
            pair.subclass === filter.subclass
        )
      )
  }
}

function summarize(rows: AnalyticsCompanyRow[]): AnalyticsSummary {
  const active = rows.filter((row) => row.tipStatus === "active").length
  const expired = rows.filter((row) => row.tipStatus === "expired").length
  const prospect = rows.filter((row) => row.tipStatus === "prospect").length

  const totalIncome = sumKnown(rows.map((row) => row.totalIncome))
  const incomes = rows
    .map((row) => row.totalIncome)
    .filter((value): value is number => value != null)
    .sort((left, right) => left - right)
  const totalPremium = sumKnown(rows.map((row) => row.premium))
  const totalLoss = sumKnown(rows.map((row) => row.loss))
  const opportunityScores = rows
    .map((row) => row.opportunityScore)
    .filter((value): value is number => value != null)

  return {
    companyCount: rows.length,
    active,
    expired,
    prospect,
    tipPenetration:
      rows.length > 0 ? ((active + expired) / rows.length) * 100 : null,
    totalIncome,
    medianIncome: median(incomes),
    totalPremium,
    totalLoss,
    lossRatio:
      totalPremium != null && totalPremium !== 0 && totalLoss != null
        ? (totalLoss / totalPremium) * 100
        : null,
    averageOpportunityScore:
      opportunityScores.length > 0
        ? opportunityScores.reduce((sum, value) => sum + value, 0) /
          opportunityScores.length
        : null,
  }
}

function distribution(
  rows: AnalyticsCompanyRow[],
  dimension: AnalyticsDerivedDistribution["dimension"]
): AnalyticsDerivedDistribution {
  const counts = new Map<string, number>()

  for (const row of rows) {
    const value =
      dimension === "tipStatus"
        ? row.tipStatus
        : dimension === "province"
          ? row.province
          : dimension === "businessType"
            ? row.businessType
            : row.incomeBand

    if (!value) continue
    counts.set(value, (counts.get(value) ?? 0) + 1)
  }

  return {
    dimension,
    buckets: [...counts.entries()]
      .map(([value, count]) => ({ value, label: value, count }))
      .sort((left, right) => right.count - left.count),
  }
}

function sumKnown(values: Array<number | null | undefined>) {
  const known = values.filter((value): value is number => value != null)
  return known.length > 0
    ? known.reduce((sum, value) => sum + value, 0)
    : null
}

function median(values: number[]) {
  if (values.length === 0) return null
  const middle = Math.floor(values.length / 2)
  if (values.length % 2 === 1) return values[middle]
  return (values[middle - 1] + values[middle]) / 2
}
