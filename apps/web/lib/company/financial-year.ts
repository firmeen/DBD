import type { CompanyFinancialYear } from "./types"

export function resolveCompanyFinancialYear(
  years: CompanyFinancialYear[],
  explicitYear: string | null,
  lensYear: number | null
) {
  if (
    explicitYear &&
    years.some((financialYear) => financialYear.year === explicitYear)
  ) {
    return explicitYear
  }

  const requestedYear = lensYear?.toString() ?? null
  if (
    requestedYear &&
    years.some((financialYear) => financialYear.year === requestedYear)
  ) {
    return requestedYear
  }

  return latestFinancialYear(years)?.year ?? null
}

export function latestFinancialYear(years: CompanyFinancialYear[]) {
  return years.reduce<CompanyFinancialYear | null>((latest, candidate) => {
    if (!latest) return candidate

    return compareFinancialYears(candidate, latest) > 0
      ? candidate
      : latest
  }, null)
}

function compareFinancialYears(
  left: CompanyFinancialYear,
  right: CompanyFinancialYear
) {
  const leftYear = numericYear(left.year)
  const rightYear = numericYear(right.year)

  if (leftYear !== null && rightYear !== null && leftYear !== rightYear) {
    return leftYear - rightYear
  }

  if (left.year !== right.year) {
    return left.year.localeCompare(right.year, undefined, {
      numeric: true,
      sensitivity: "base",
    })
  }

  const leftEnd = left.fiscalYearEndDate ?? ""
  const rightEnd = right.fiscalYearEndDate ?? ""
  return leftEnd.localeCompare(rightEnd)
}

function numericYear(value: string) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}
