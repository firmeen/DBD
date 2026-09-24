import { describe, expect, it } from "vitest"

import {
  defaultCompanySurfaceSnapshot,
  parseCompanySurfaceSnapshot,
  resolveCompanySurfaceSnapshot,
} from "./surface-snapshot"

describe("company surface snapshot", () => {
  it("round-trips company workspace state", () => {
    const snapshot = defaultCompanySurfaceSnapshot()
    expect(parseCompanySurfaceSnapshot(snapshot)).toEqual(snapshot)
  })

  it("restores company-local state only for the same focused company", () => {
    const snapshot = {
      focusedCompanyId: "0100000000001",
      view: "history" as const,
      searchTerm: "legacy search",
      financialYear: "2025",
    }

    expect(
      resolveCompanySurfaceSnapshot(snapshot, "0100000000001")
    ).toEqual(snapshot)

    expect(
      resolveCompanySurfaceSnapshot(snapshot, "0100000000002")
    ).toEqual({
      focusedCompanyId: "0100000000002",
      view: "dossier",
      searchTerm: "",
      financialYear: null,
    })
  })

  it("migrates legacy snapshots without an entity binding to discovery state", () => {
    const legacy = {
      view: "history",
      searchTerm: "legacy search",
      financialYear: "2024",
    }

    expect(parseCompanySurfaceSnapshot(legacy)?.focusedCompanyId).toBeNull()
    expect(resolveCompanySurfaceSnapshot(legacy, "0100000000003")).toEqual({
      focusedCompanyId: "0100000000003",
      view: "dossier",
      searchTerm: "",
      financialYear: null,
    })
  })

  it("rejects unsupported analytical views", () => {
    expect(
      parseCompanySurfaceSnapshot({
        ...defaultCompanySurfaceSnapshot(),
        view: "radar-dashboard",
      })
    ).toBeNull()
  })
})
