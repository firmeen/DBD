import { describe, expect, it } from "vitest"

import { capMatrixRegionSelection } from "./matrix-selection"
import type { EntityRef } from "@/lib/intelligence/types"

function companies(count: number): EntityRef[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `company:${index}`,
    kind: "company" as const,
    label: `Company ${index}`,
  }))
}

function people(count: number): EntityRef[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `person:${index}`,
    kind: "person" as const,
    label: `Person ${index}`,
  }))
}

describe("Board Matrix region selection budget", () => {
  it("preserves both axes when one axis alone exceeds the cap", () => {
    const selection = capMatrixRegionSelection(
      companies(250),
      people(1),
      240
    )

    expect(selection).toHaveLength(240)
    expect(selection.some((entity) => entity.kind === "company")).toBe(true)
    expect(selection.some((entity) => entity.kind === "person")).toBe(true)
  })

  it("uses spare capacity from a small axis on the larger axis", () => {
    const selection = capMatrixRegionSelection(
      companies(230),
      people(10),
      240
    )

    expect(selection).toHaveLength(240)
    expect(selection.filter((entity) => entity.kind === "company")).toHaveLength(230)
    expect(selection.filter((entity) => entity.kind === "person")).toHaveLength(10)
  })

  it("does not truncate a region already within budget", () => {
    expect(
      capMatrixRegionSelection(companies(3), people(2), 240)
    ).toHaveLength(5)
  })
})
