import { describe, expect, it } from "vitest"

import {
  defaultRelationshipSurfaceSnapshot,
  parseRelationshipSurfaceSnapshot,
  resolvePeopleSurfaceSnapshot,
} from "./surface-snapshot"

describe("relationship surface snapshots", () => {
  it("preserves people-local search only for the same person focus", () => {
    const snapshot = {
      focusedPersonId: "person:1",
      searchTerm: "director",
    }

    expect(resolvePeopleSurfaceSnapshot(snapshot, "person:1")).toEqual(snapshot)
    expect(resolvePeopleSurfaceSnapshot(snapshot, "person:2")).toEqual({
      focusedPersonId: "person:2",
      searchTerm: "",
    })
  })

  it("rejects unsupported relationship views and unsafe depth", () => {
    expect(
      parseRelationshipSurfaceSnapshot({
        ...defaultRelationshipSurfaceSnapshot(),
        view: "decorative-3d",
      })
    ).toEqual(defaultRelationshipSurfaceSnapshot())

    expect(
      parseRelationshipSurfaceSnapshot({
        ...defaultRelationshipSurfaceSnapshot(),
        depth: 99,
      })
    ).toEqual(defaultRelationshipSurfaceSnapshot())
  })
})
