import { describe, expect, it } from "vitest"

import {
  pathEdgeIds,
  relationshipNeighbourhood,
  shortestRelationshipPath,
} from "./graph-algorithms"

const snapshot = {
  nodes: [
    { id: "c1", kind: "company" as const, label: "C1", degree: 1 },
    { id: "p1", kind: "person" as const, label: "P1", degree: 2 },
    { id: "c2", kind: "company" as const, label: "C2", degree: 2 },
    { id: "p2", kind: "person" as const, label: "P2", degree: 1 },
  ],
  edges: [
    { id: "e1", source: "c1", target: "p1", kind: "board" as const },
    { id: "e2", source: "p1", target: "c2", kind: "board" as const },
    { id: "e3", source: "c2", target: "p2", kind: "board" as const },
  ],
}

describe("relationship graph algorithms", () => {
  it("finds a shortest loaded-graph path", () => {
    expect(shortestRelationshipPath(snapshot, "c1", "p2")).toEqual([
      "c1",
      "p1",
      "c2",
      "p2",
    ])
  })

  it("returns an empty path for missing entities", () => {
    expect(shortestRelationshipPath(snapshot, "c1", "missing")).toEqual([])
  })

  it("bounds focused neighbourhoods by hop count", () => {
    const oneHop = relationshipNeighbourhood(snapshot, "p1", 1)
    expect([...oneHop.nodeIds]).toEqual(["p1", "c1", "c2"])
    expect([...oneHop.edgeIds].sort()).toEqual(["e1", "e2"])
  })

  it("maps a node path back to relationship edge IDs", () => {
    expect(
      [...pathEdgeIds(snapshot.edges, ["c1", "p1", "c2"])].sort()
    ).toEqual(["e1", "e2"])
  })
})
