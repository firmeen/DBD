import { describe, expect, it } from "vitest"

import {
  defaultCompanySurfaceSnapshot,
  parseCompanySurfaceSnapshot,
} from "./surface-snapshot"

describe("company surface snapshot", () => {
  it("round-trips company workspace state", () => {
    const snapshot = defaultCompanySurfaceSnapshot()
    expect(parseCompanySurfaceSnapshot(snapshot)).toEqual(snapshot)
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
