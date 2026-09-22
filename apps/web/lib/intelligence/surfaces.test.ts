import { describe, expect, it } from "vitest"

import { SURFACES, SURFACE_BY_ID, isSurfaceId } from "./surfaces"

describe("intelligence surface registry", () => {
  it("keeps every keyboard namespace unique", () => {
    expect(new Set(SURFACES.map((surface) => surface.id)).size).toBe(
      SURFACES.length
    )
  })

  it("resolves all registered surfaces", () => {
    for (const surface of SURFACES) {
      expect(SURFACE_BY_ID[surface.id]).toBe(surface)
      expect(isSurfaceId(surface.id)).toBe(true)
    }
  })

  it("keeps the type guard exact", () => {
    expect(isSurfaceId("m")).toBe(false)
    expect(isSurfaceId("X")).toBe(false)
    expect(isSurfaceId("1")).toBe(false)
  })
})
