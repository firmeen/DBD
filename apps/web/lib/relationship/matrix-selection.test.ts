import { describe, expect, it } from "vitest"

import { matrixRegionSelection } from "./matrix-selection"
import type { MatrixCompany, MatrixPerson } from "./types"

const company = (index: number): MatrixCompany => ({
  id: `company:${index}`,
  name: `Company ${index}`,
  degree: 1,
})

const person = (index: number): MatrixPerson => ({
  id: `person:${index}`,
  name: `Person ${index}`,
  degree: 1,
})

describe("Board Matrix region selection", () => {
  it("preserves both axes when companies exceed the global cap", () => {
    const selection = matrixRegionSelection(
      Array.from({ length: 250 }, (_, index) => company(index)),
      [person(1)],
      240
    )

    expect(selection).toHaveLength(240)
    expect(selection.some((entity) => entity.kind === "company")).toBe(true)
    expect(selection.some((entity) => entity.kind === "person")).toBe(true)
  })

  it("redistributes unused budget to the larger axis", () => {
    const selection = matrixRegionSelection(
      [company(1), company(2)],
      Array.from({ length: 400 }, (_, index) => person(index)),
      240
    )

    expect(selection).toHaveLength(240)
    expect(
      selection.filter((entity) => entity.kind === "company")
    ).toHaveLength(2)
    expect(
      selection.filter((entity) => entity.kind === "person")
    ).toHaveLength(238)
  })
})
