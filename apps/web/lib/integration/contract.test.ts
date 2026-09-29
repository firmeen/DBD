import { describe, expect, it } from "vitest"

import {
  isSafeRelativeReturnPath,
  parseIntegrationLaunchRequest,
} from "./contract"

describe("TIPX integration v1 contract", () => {
  it("accepts a canonical Central launch request", () => {
    const request = parseIntegrationLaunchRequest({
      version: "v1",
      sourceApp: "central",
      target: {
        surface: "C",
        entity: {
          kind: "company",
          id: "0100000000000",
          labelHint: "Example Company",
        },
      },
      lens: {
        province: "Bangkok",
        fiscalYear: 2025,
      },
      returnContext: {
        path: "/companies/0100000000000",
        label: "Back to Central",
      },
    })

    expect(request.sourceApp).toBe("central")
    expect(request.target?.entity?.id).toBe("0100000000000")
  })

  it("rejects user and permission claims in the launch payload", () => {
    expect(() =>
      parseIntegrationLaunchRequest({
        version: "v1",
        sourceApp: "central",
        user: { id: "admin" },
        permissions: ["all"],
      })
    ).toThrow()
  })

  it("rejects unsafe external or protocol-relative return paths", () => {
    expect(isSafeRelativeReturnPath("/companies/1")).toBe(true)
    expect(isSafeRelativeReturnPath("https://evil.example")).toBe(false)
    expect(isSafeRelativeReturnPath("//evil.example/path")).toBe(false)
    expect(isSafeRelativeReturnPath("/\\evil.example")).toBe(false)
  })
})
