import { describe, expect, it } from "vitest"

import { parseSpatialSnapshot } from "./contract"

function snapshotWithCoordinates(coordinates: [number, number]) {
  return {
    availability: "ready" as const,
    generatedAt: null,
    capabilities: {
      administrativeGeometry: false,
      preciseCompanyGeometry: true,
      branches: false,
      h3Aggregates: false,
      opportunityOverlay: false,
    },
    totals: {
      companies: 1,
      active: 1,
      expired: 0,
      prospect: 0,
    },
    companies: [
      {
        id: "0100000000000",
        name: "Example Company",
        coordinates,
        tipStatus: "active" as const,
      },
    ],
    branches: [],
    h3Cells: [],
    areas: [],
  }
}

describe("spatial serving contract", () => {
  it("accepts finite in-range geographic coordinates", () => {
    expect(
      parseSpatialSnapshot(snapshotWithCoordinates([100.5018, 13.7563]))
        .companies[0]?.coordinates
    ).toEqual([100.5018, 13.7563])
  })

  it("rejects longitude outside the geographic domain", () => {
    expect(() =>
      parseSpatialSnapshot(snapshotWithCoordinates([181, 13.7563]))
    ).toThrow()
  })

  it("rejects latitude outside the geographic domain", () => {
    expect(() =>
      parseSpatialSnapshot(snapshotWithCoordinates([100.5018, 91]))
    ).toThrow()
  })

  it("rejects non-finite coordinates before map engines receive them", () => {
    expect(() =>
      parseSpatialSnapshot(snapshotWithCoordinates([Number.POSITIVE_INFINITY, 13]))
    ).toThrow()
  })
})
