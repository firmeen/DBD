import { describe, expect, it } from "vitest"

import {
  buildTipxLaunchHref,
  buildTipxReturnEndpointHref,
  normalizeIntegrationLaunchRequest,
  parseTipxIntegrationSearchParams,
} from "./launch-url"

describe("TIPX integration launch URL", () => {
  it("builds a versioned company deep link without sensitive identity data", () => {
    const context = normalizeIntegrationLaunchRequest({
      version: "v1",
      sourceApp: "central",
      target: {
        entity: {
          kind: "company",
          id: "0100000000000",
          labelHint: "Example Company",
        },
      },
      lens: {
        province: "Bangkok",
        tipStatus: "prospect",
      },
      returnContext: {
        path: "/companies/0100000000000",
      },
    })

    const url = new URL(
      buildTipxLaunchHref(context, "https://tipx.example.com/")
    )

    expect(url.searchParams.get("tipx_source")).toBe("central")
    expect(url.searchParams.get("tipx_surface")).toBe("C")
    expect(url.searchParams.get("tipx_entity_id")).toBe("0100000000000")
    expect(url.searchParams.get("province")).toBe("Bangkok")
    expect(url.searchParams.get("tipStatus")).toBe("prospect")
    expect(url.searchParams.has("user")).toBe(false)
    expect(url.searchParams.has("permissions")).toBe(false)
  })

  it("builds a local return endpoint without exposing the source origin", () => {
    const context = normalizeIntegrationLaunchRequest({
      version: "v1",
      sourceApp: "central",
      returnContext: {
        path: "/companies/0100000000000",
      },
    })

    const href = buildTipxReturnEndpointHref(context)

    expect(href).toContain("/api/tipx/integration/v1/return?")
    expect(href).toContain("source=central")
    expect(href).toContain("path=%2Fcompanies%2F0100000000000")
    expect(href).not.toContain("central.example.com")
  })

  it("round-trips launch context from page search params", () => {
    const context = parseTipxIntegrationSearchParams({
      tipx_source: "central",
      tipx_surface: "P",
      tipx_entity_kind: "person",
      tipx_entity_id: "person-42",
      tipx_entity_label: "Director 42",
      tipx_return_path: "/people/42",
      fiscalYear: "2025",
    })

    expect(context).toEqual({
      version: "v1",
      sourceApp: "central",
      target: {
        surface: "P",
        entity: {
          kind: "person",
          id: "person-42",
          labelHint: "Director 42",
        },
      },
      lens: {
        fiscalYear: 2025,
      },
      returnContext: {
        path: "/people/42",
      },
    })
  })

  it("ignores malformed integration query context safely", () => {
    expect(
      parseTipxIntegrationSearchParams({
        tipx_source: "Central With Spaces",
        tipx_surface: "C",
      })
    ).toBeNull()
  })
})
