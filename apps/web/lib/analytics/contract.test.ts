import { describe, expect, it } from "vitest"

import { parseAnalyticsSnapshot } from "./contract"

function baseSnapshot() {
  return {
    availability: "ready" as const,
    generatedAt: null,
    population: null,
    capabilities: {
      populationAnalytics: true,
      opportunity: true,
      productPerformance: true,
      territory: true,
      localCrossFilter: true,
    },
    summary: {
      companyCount: 1,
      active: 1,
      expired: 0,
      prospect: 0,
      tipPenetration: 100,
      totalIncome: 100,
      medianIncome: 100,
      totalPremium: 10,
      totalLoss: 2,
      lossRatio: 20,
      averageOpportunityScore: 0.8,
    },
    distributions: [],
    companies: [
      {
        id: "c1",
        name: "Company 1",
        opportunityScore: 0.8,
        products: [],
        subclasses: [],
      },
    ],
    products: [],
    territories: [],
    series: [],
  }
}

describe("analytics serving contract", () => {
  it("accepts a bounded semantic analytics snapshot", () => {
    expect(parseAnalyticsSnapshot(baseSnapshot()).companies[0]?.id).toBe("c1")
  })

  it("rejects duplicate company identities", () => {
    const snapshot = baseSnapshot()
    expect(() =>
      parseAnalyticsSnapshot({
        ...snapshot,
        companies: [...snapshot.companies, snapshot.companies[0]],
      })
    ).toThrow(/duplicate companies IDs/)
  })

  it("rejects opportunity scores outside 0..1", () => {
    const snapshot = baseSnapshot()
    expect(() =>
      parseAnalyticsSnapshot({
        ...snapshot,
        companies: [
          {
            ...snapshot.companies[0],
            opportunityScore: 1.1,
          },
        ],
      })
    ).toThrow()
  })

  it("rejects oversized serving messages", () => {
    expect(() =>
      parseAnalyticsSnapshot({
        ...baseSnapshot(),
        message: "x".repeat(2_001),
      })
    ).toThrow()
  })

  it("rejects duplicate bucket identities within a distribution", () => {
    expect(() =>
      parseAnalyticsSnapshot({
        ...baseSnapshot(),
        distributions: [
          {
            id: "province",
            label: "Province",
            format: "number",
            buckets: [
              { id: "bkk", label: "Bangkok", count: 1 },
              { id: "bkk", label: "Bangkok duplicate", count: 1 },
            ],
          },
        ],
      })
    ).toThrow(/duplicate bucket IDs/)
  })
})
