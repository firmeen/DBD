import { describe, expect, it } from "vitest"

import {
  defaultAnalyticsSurfaceSnapshot,
  parseAnalyticsSurfaceSnapshot,
} from "./surface-snapshot"

describe("analytics surface snapshot", () => {
  it("restores a snapshot only for the matching analytical mode", () => {
    const population = {
      mode: "population" as const,
      filters: [
        {
          dimension: "province" as const,
          value: "Bangkok",
          label: "Bangkok",
        },
      ],
      candidateRailOpen: false,
    }

    expect(
      parseAnalyticsSurfaceSnapshot(population, "population")
    ).toEqual(population)

    expect(
      parseAnalyticsSurfaceSnapshot(population, "opportunity")
    ).toEqual(defaultAnalyticsSurfaceSnapshot("opportunity"))
  })

  it("rejects oversized local filter stacks", () => {
    const filters = Array.from({ length: 13 }, (_, index) => ({
      dimension: "province" as const,
      value: `P${index}`,
      label: `Province ${index}`,
    }))

    expect(
      parseAnalyticsSurfaceSnapshot(
        {
          mode: "population",
          filters,
          candidateRailOpen: true,
        },
        "population"
      )
    ).toEqual(defaultAnalyticsSurfaceSnapshot("population"))
  })
})
