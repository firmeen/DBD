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
      expect(isSurfaceId(surface.id.toLowerCase())).toBe(true)
    }
  })

  it("rejects unrelated keyboard keys", () => {
    expect(isSurfaceId("X")).toBe(false)
    expect(isSurfaceId("1")).toBe(false)
  })
})
