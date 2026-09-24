import { describe, expect, it } from "vitest"

import { normalizeOwnershipComposition } from "./ownership"

describe("ownership composition normalization", () => {
  it("normalizes complete TH/OTHER composition", () => {
    expect(normalizeOwnershipComposition(60, 40)).toEqual({
      thaiPct: 60,
      otherPct: 40,
      thaiWidth: 60,
      otherWidth: 40,
    })
  })

  it("does not invent the missing side of a partial composition", () => {
    expect(normalizeOwnershipComposition(60, null)).toBeNull()
    expect(normalizeOwnershipComposition(null, 40)).toBeNull()
  })

  it("treats an empty 0/0 composition as unavailable", () => {
    expect(normalizeOwnershipComposition(0, 0)).toBeNull()
  })
})
