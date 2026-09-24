import { describe, expect, it } from "vitest"

import {
  parseBoardMatrixSnapshot,
  parseCompanyBoardSnapshot,
  parseGenealogySnapshot,
  parsePersonDetailSnapshot,
  parseRelationshipGraphSnapshot,
} from "./contract"

describe("relationship serving contracts", () => {
  it("binds person detail to the requested canonical person", () => {
    const payload = {
      availability: "ready",
      profile: {
        id: "person:1",
        name: "Example Director",
        companyCount: 1,
      },
      companies: [],
      sharedDirectors: [],
    }

    expect(parsePersonDetailSnapshot(payload, "person:1").profile?.id).toBe(
      "person:1"
    )
    expect(() => parsePersonDetailSnapshot(payload, "person:2")).toThrow(
      /identity mismatch/
    )
  })

  it("binds company-board responses to the requested company", () => {
    const payload = {
      availability: "ready",
      company: {
        id: "0100000000000",
        kind: "company",
        label: "Example Company",
      },
      members: [],
    }

    expect(
      parseCompanyBoardSnapshot(payload, "0100000000000").company?.id
    ).toBe("0100000000000")
    expect(() =>
      parseCompanyBoardSnapshot(payload, "0100000000001")
    ).toThrow(/identity mismatch/)
  })

  it("rejects graph edges that reference missing nodes", () => {
    expect(() =>
      parseRelationshipGraphSnapshot({
        availability: "ready",
        layout: "server",
        nodes: [
          {
            id: "company:1",
            kind: "company",
            label: "Company 1",
            degree: 1,
            x: 0,
            y: 0,
          },
        ],
        edges: [
          {
            id: "edge:1",
            source: "company:1",
            target: "person:missing",
            kind: "board",
          },
        ],
      })
    ).toThrow()
  })

  it("requires coordinates when a graph claims server layout", () => {
    expect(() =>
      parseRelationshipGraphSnapshot({
        availability: "ready",
        layout: "server",
        nodes: [
          {
            id: "company:1",
            kind: "company",
            label: "Company 1",
            degree: 0,
          },
        ],
        edges: [],
      })
    ).toThrow(/require finite x\/y/)
  })

  it("rejects duplicate Matrix axis identities", () => {
    expect(() =>
      parseBoardMatrixSnapshot({
        availability: "ready",
        companies: [
          { id: "company:1", name: "Company 1", degree: 1 },
          { id: "company:1", name: "Company 1 duplicate", degree: 1 },
        ],
        people: [
          { id: "person:1", name: "Person 1", degree: 1 },
          { id: "person:1", name: "Person 1 duplicate", degree: 1 },
        ],
        memberships: [],
      })
    ).toThrow()
  })

  it("rejects matrix memberships with missing axis entities", () => {
    expect(() =>
      parseBoardMatrixSnapshot({
        availability: "ready",
        companies: [
          {
            id: "company:1",
            name: "Company 1",
            degree: 1,
          },
        ],
        people: [],
        memberships: [
          {
            companyId: "company:1",
            personId: "person:missing",
          },
        ],
      })
    ).toThrow()
  })

  it("rejects duplicate genealogy node and edge identities", () => {
    expect(() =>
      parseGenealogySnapshot({
        availability: "ready",
        nodes: [
          {
            id: "g1",
            company: {
              id: "company:1",
              kind: "company",
              label: "Company 1",
            },
          },
          {
            id: "g1",
            company: {
              id: "company:2",
              kind: "company",
              label: "Company 2",
            },
          },
        ],
        edges: [
          {
            id: "e1",
            source: "g1",
            target: "g1",
            kind: "transform",
          },
          {
            id: "e1",
            source: "g1",
            target: "g1",
            kind: "merger",
          },
        ],
      })
    ).toThrow()
  })

  it("rejects oversized relationship text payloads", () => {
    expect(() =>
      parsePersonDetailSnapshot({
        availability: "ready",
        message: "x".repeat(2_001),
        profile: null,
        companies: [],
        sharedDirectors: [],
      })
    ).toThrow()
  })
})
