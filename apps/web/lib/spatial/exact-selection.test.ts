import { describe, expect, it } from "vitest"

import type { EntityRef } from "@/lib/intelligence/types"

import { exactCompanyIdsForSpatial } from "./exact-selection"
import { MAX_SPATIAL_EXACT_COMPANY_IDS } from "./types"

const company = (id: string): EntityRef => ({
  id,
  kind: "company",
  label: id,
})

describe("spatial exact company handoff", () => {
  it("preserves unique company IDs from shared Selection", () => {
    expect(
      exactCompanyIdsForSpatial([
        company("c1"),
        company("c1"),
        company("c2"),
        { id: "p1", kind: "person", label: "Person 1" },
      ])
    ).toEqual(["c1", "c2"])
  })

  it("rejects an oversized exact handoff instead of truncating silently", () => {
    const selection = Array.from(
      { length: MAX_SPATIAL_EXACT_COMPANY_IDS + 1 },
      (_, index) => company(`c${index}`)
    )

    expect(exactCompanyIdsForSpatial(selection)).toEqual([])
  })
})
