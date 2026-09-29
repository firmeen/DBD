import { describe, expect, it } from "vitest"

import { TIPX_RENDER_PALETTES } from "./palette"

describe("TIPX render palettes", () => {
  it("keeps Light and Dark renderer fields distinct", () => {
    expect(TIPX_RENDER_PALETTES.light.graph.canvas).not.toBe(
      TIPX_RENDER_PALETTES.dark.graph.canvas
    )
    expect(TIPX_RENDER_PALETTES.light.chart.tooltipBackground).not.toBe(
      TIPX_RENDER_PALETTES.dark.chart.tooltipBackground
    )
    expect(TIPX_RENDER_PALETTES.light.deck.clusterFill).not.toEqual(
      TIPX_RENDER_PALETTES.dark.deck.clusterFill
    )
  })

  it("keeps brand analytical blue separate from accent red", () => {
    for (const palette of Object.values(TIPX_RENDER_PALETTES)) {
      expect(palette.chart.primary).not.toBe(palette.chart.accent)
      expect(palette.graph.selected).not.toBe(palette.graph.path)
    }
  })

  it("keeps semantic entity states distinct from selection", () => {
    for (const palette of Object.values(TIPX_RENDER_PALETTES)) {
      expect(new Set([
        palette.entity.active,
        palette.entity.expired,
        palette.entity.prospect,
        palette.entity.selected,
      ]).size).toBe(4)
    }
  })
})
