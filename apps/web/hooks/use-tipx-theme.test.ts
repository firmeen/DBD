import { describe, expect, it } from "vitest"

import { TIPX_RENDER_PALETTES } from "@/lib/theme/palette"

import { resolveTipxThemeSnapshot } from "./use-tipx-theme"

describe("TIPX theme hydration snapshot", () => {
  it("keeps the pre-hydration snapshot deterministic even when client theme is already known", () => {
    expect(
      resolveTipxThemeSnapshot({
        theme: "dark",
        resolvedTheme: "dark",
        mounted: false,
      })
    ).toEqual({
      appearance: "system",
      resolved: "dark",
      palette: TIPX_RENDER_PALETTES.dark,
    })

    expect(
      resolveTipxThemeSnapshot({
        theme: "light",
        resolvedTheme: "light",
        mounted: false,
      })
    ).toEqual({
      appearance: "system",
      resolved: "dark",
      palette: TIPX_RENDER_PALETTES.dark,
    })
  })

  it("exposes the persisted appearance after hydration", () => {
    expect(
      resolveTipxThemeSnapshot({
        theme: "dark",
        resolvedTheme: "dark",
        mounted: true,
      }).appearance
    ).toBe("dark")

    expect(
      resolveTipxThemeSnapshot({
        theme: "light",
        resolvedTheme: "light",
        mounted: true,
      }).resolved
    ).toBe("light")
  })

  it("preserves system mode while resolving its concrete palette after hydration", () => {
    const snapshot = resolveTipxThemeSnapshot({
      theme: "system",
      resolvedTheme: "light",
      mounted: true,
    })

    expect(snapshot.appearance).toBe("system")
    expect(snapshot.resolved).toBe("light")
    expect(snapshot.palette).toBe(TIPX_RENDER_PALETTES.light)
  })
})
