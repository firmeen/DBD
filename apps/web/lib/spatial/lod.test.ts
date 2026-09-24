import { describe, expect, it } from "vitest"

import { h3ResolutionForZoom, spatialLodForZoom } from "./lod"

describe("spatial semantic LOD", () => {
  it("changes representation by scale instead of marker size", () => {
    expect(spatialLodForZoom(4)).toBe("national")
    expect(spatialLodForZoom(7)).toBe("regional")
    expect(spatialLodForZoom(10)).toBe("local")
    expect(spatialLodForZoom(14)).toBe("entity")
  })

  it("increases H3 resolution as the camera moves closer", () => {
    expect(h3ResolutionForZoom(4)).toBeLessThan(h3ResolutionForZoom(8))
    expect(h3ResolutionForZoom(8)).toBeLessThan(h3ResolutionForZoom(11))
    expect(h3ResolutionForZoom(11)).toBeLessThan(h3ResolutionForZoom(14))
  })
})
