import { describe, expect, it } from "vitest"

import { computeAnalyticsSubset } from "./compute"
import type { AnalyticsCompanyRow } from "./types"

const rows: AnalyticsCompanyRow[] = [
  {
    id: "c1",
    name: "A",
    province: "Bangkok",
    businessType: "Manufacturing",
    incomeBand: "100M+",
    tipStatus: "active",
    totalIncome: 200,
    premium: 10,
    loss: 2,
    opportunityScore: 0.8,
    products: ["Motor"],
    subclasses: ["Fleet"],
  },
  {
    id: "c2",
    name: "B",
    province: "Bangkok",
    businessType: "Retail",
    incomeBand: "10M-100M",
    tipStatus: "prospect",
    totalIncome: 100,
    premium: null,
    loss: null,
    opportunityScore: 0.6,
    products: [],
    subclasses: [],
  },
]

describe("analytics cross-filter compute", () => {
  it("filters loaded company rows and recomputes coordinated summaries", () => {
    const result = computeAnalyticsSubset(rows, [
      { dimension: "province", value: "Bangkok", label: "Bangkok" },
      {
        dimension: "businessType",
        value: "Manufacturing",
        label: "Manufacturing",
      },
    ])

    expect(result.companyIds).toEqual(["c1"])
    expect(result.summary.companyCount).toBe(1)
    expect(result.summary.active).toBe(1)
    expect(result.summary.tipPenetration).toBe(100)
    expect(result.summary.lossRatio).toBe(20)
  })

  it("supports product and subclass membership filters", () => {
    expect(
      computeAnalyticsSubset(rows, [
        { dimension: "product", value: "Motor", label: "Motor" },
        { dimension: "subclass", value: "Fleet", label: "Fleet" },
      ]).companyIds
    ).toEqual(["c1"])
  })
})
