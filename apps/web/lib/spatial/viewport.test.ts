import { describe, expect, it } from "vitest"

import { THAILAND_VIEW } from "./surface-snapshot"
import { spatialBoundsForView } from "./viewport"

describe("spatial viewport bounds", () => {
  it("returns a finite geographic envelope for the visible surface", () => {
    const bounds = spatialBoundsForView(THAILAND_VIEW, 1440, 900)

    expect(bounds).not.toBeNull()
    expect(bounds?.every(Number.isFinite)).toBe(true)
    expect(bounds?.[0]).toBeLessThan(THAILAND_VIEW.longitude)
    expect(bounds?.[2]).toBeGreaterThan(THAILAND_VIEW.longitude)
    expect(bounds?.[1]).toBeLessThan(THAILAND_VIEW.latitude)
    expect(bounds?.[3]).toBeGreaterThan(THAILAND_VIEW.latitude)
  })

  it("does not invent bounds before the map surface has dimensions", () => {
    expect(spatialBoundsForView(THAILAND_VIEW, 0, 900)).toBeNull()
    expect(spatialBoundsForView(THAILAND_VIEW, 1440, 0)).toBeNull()
  })
})
