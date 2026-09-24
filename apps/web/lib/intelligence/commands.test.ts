import { describe, expect, it } from "vitest"

import { INTELLIGENCE_COMMANDS } from "./commands"
import { SURFACES } from "./surfaces"

describe("intelligence command registry", () => {
  it("exposes exactly one navigation command per surface", () => {
    const navigation = INTELLIGENCE_COMMANDS.filter(
      (command) => command.category === "Navigate"
    )

    expect(navigation).toHaveLength(SURFACES.length)
    expect(navigation.map((command) => command.surface)).toEqual(
      SURFACES.map((surface) => surface.id)
    )
  })

  it("exposes System, Light and Dark as explicit appearance commands", () => {
    const appearance = INTELLIGENCE_COMMANDS.filter(
      (command) => command.category === "Appearance"
    )

    expect(appearance.map((command) => command.id)).toEqual([
      "theme.system",
      "theme.light",
      "theme.dark",
    ])
  })

  it("does not duplicate command ids", () => {
    const ids = INTELLIGENCE_COMMANDS.map((command) => command.id)
    expect(new Set(ids).size).toBe(ids.length)
  })
})
