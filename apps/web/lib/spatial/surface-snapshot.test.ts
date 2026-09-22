import { describe, expect, it } from "vitest"

import {
  defaultSpatialSurfaceSnapshot,
  parseSpatialSurfaceSnapshot,
} from "./surface-snapshot"

describe("spatial surface snapshot", () => {
  it("round-trips the supported persistent spatial state", () => {
    const snapshot = defaultSpatialSurfaceSnapshot()
    expect(parseSpatialSurfaceSnapshot(snapshot)).toEqual(snapshot)
  })

  it("rejects unsafe camera and radius state", () => {
    expect(
      parseSpatialSurfaceSnapshot({
        ...defaultSpatialSurfaceSnapshot(),
        radiusKm: 1000,
      })
    ).toBeNull()
  })
})
