import { describe, expect, it } from "vitest"

import {
  latestFinancialYear,
  resolveCompanyFinancialYear,
} from "./financial-year"
import type { CompanyFinancialYear } from "./types"

function year(
  value: string,
  fiscalYearEndDate?: string
): CompanyFinancialYear {
  return {
    year: value,
    fiscalYearEndDate,
    ratios: [],
    lineItems: [],
  }
}

describe("company fiscal-year resolution", () => {
  it("prefers an explicit valid year over all fallbacks", () => {
    const years = [year("2023"), year("2025"), year("2024")]
    expect(resolveCompanyFinancialYear(years, "2024", 2025)).toBe("2024")
  })

  it("uses the Lens year when no explicit valid year exists", () => {
    const years = [year("2023"), year("2025"), year("2024")]
    expect(resolveCompanyFinancialYear(years, null, 2024)).toBe("2024")
  })

  it("selects the latest fiscal year independent of response ordering", () => {
    const oldestFirst = [year("2022"), year("2023"), year("2025"), year("2024")]
    const newestFirst = [...oldestFirst].reverse()

    expect(resolveCompanyFinancialYear(oldestFirst, null, null)).toBe("2025")
    expect(resolveCompanyFinancialYear(newestFirst, null, null)).toBe("2025")
  })

  it("uses fiscal-year end date as a deterministic tie breaker", () => {
    expect(
      latestFinancialYear([
        year("FY-A", "2024-06-30"),
        year("FY-A", "2024-12-31"),
      ])?.fiscalYearEndDate
    ).toBe("2024-12-31")
  })
})
