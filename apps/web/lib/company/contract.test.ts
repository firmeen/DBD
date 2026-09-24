import { describe, expect, it } from "vitest"

import {
  parseCompanyDetailSnapshot,
  parseCompanySearchSnapshot,
} from "./contract"

function baseDetail() {
  return {
    availability: "ready" as const,
    generatedAt: null,
    capabilities: {
      financials: true,
      industryBenchmark: true,
      nationality: true,
      corporateHistory: true,
      tipContext: true,
      boardSummary: true,
    },
    profile: {
      id: "0100000000000",
      nameTh: "บริษัท ตัวอย่าง จำกัด",
      registeredCapital: 10_000_000,
    },
    financial: {
      years: [],
      series: [],
    },
    industry: null,
    nationality: {
      current: [],
      history: [],
    },
    history: [],
    tipContext: null,
    boardSummary: null,
  }
}

describe("company serving contract", () => {
  it("accepts a semantic company snapshot", () => {
    expect(parseCompanyDetailSnapshot(baseDetail()).profile?.id).toBe(
      "0100000000000"
    )
  })

  it("rejects non-finite financial values", () => {
    const detail = baseDetail()
    detail.profile.registeredCapital = Number.POSITIVE_INFINITY

    expect(() => parseCompanyDetailSnapshot(detail)).toThrow()
  })

  it("rejects nationality percentages outside 0..100", () => {
    const detail = baseDetail()
    const invalid = {
      ...detail,
      nationality: {
        ...detail.nationality,
        current: [
          {
            code: "TH",
            country: "Thailand",
            sharePct: 101,
          },
        ],
      },
    }

    expect(() => parseCompanyDetailSnapshot(invalid)).toThrow()
  })

  it("caps search populations at the UI serving budget", () => {
    const results = Array.from({ length: 51 }, (_, index) => ({
      id: String(index),
      nameTh: `Company ${index}`,
    }))

    expect(() =>
      parseCompanySearchSnapshot({
        availability: "ready",
        results,
      })
    ).toThrow()
  })
})
