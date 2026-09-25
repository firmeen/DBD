import { describe, expect, it } from "vitest"

import {
  isIntegrationPreflightOriginAllowed,
  isIntegrationRequestOriginAllowed,
  isIntegrationSourceEnabled,
  resolveIntegrationReturnUrl,
} from "./source-registry"

const ENV = {
  TIPX_INTEGRATION_SOURCES: "central,sales",
  TIPX_INTEGRATION_SOURCE_ORIGINS: JSON.stringify({
    central: ["https://central.example.com"],
    sales: ["https://sales.example.com"],
  }),
  TIPX_INTEGRATION_RETURN_ORIGINS: JSON.stringify({
    central: "https://central.example.com",
    sales: "https://sales.example.com",
  }),
}

describe("TIPX integration source registry", () => {
  it("enables configured source applications without changing the contract", () => {
    expect(isIntegrationSourceEnabled("central", ENV)).toBe(true)
    expect(isIntegrationSourceEnabled("sales", ENV)).toBe(true)
    expect(isIntegrationSourceEnabled("unknown", ENV)).toBe(false)
  })

  it("binds browser origins to the declared source application", () => {
    const requestUrl = "https://tipx.example.com/api/tipx/integration/v1/launch"

    expect(
      isIntegrationRequestOriginAllowed(
        "central",
        "https://central.example.com",
        requestUrl,
        ENV
      )
    ).toBe(true)

    expect(
      isIntegrationRequestOriginAllowed(
        "central",
        "https://sales.example.com",
        requestUrl,
        ENV
      )
    ).toBe(false)
  })

  it("resolves return navigation only against trusted source origins", () => {
    expect(
      resolveIntegrationReturnUrl(
        "central",
        "/companies/1?tab=detail",
        ENV
      )
    ).toBe("https://central.example.com/companies/1?tab=detail")

    expect(
      resolveIntegrationReturnUrl(
        "central",
        "//evil.example/path",
        ENV
      )
    ).toBeNull()

    expect(
      resolveIntegrationReturnUrl(
        "unknown",
        "/companies/1",
        ENV
      )
    ).toBeNull()
  })

  it("allows preflight only from a configured integration origin", () => {
    const requestUrl = "https://tipx.example.com/api/tipx/integration/v1/launch"

    expect(
      isIntegrationPreflightOriginAllowed(
        "https://sales.example.com",
        requestUrl,
        ENV
      )
    ).toBe(true)

    expect(
      isIntegrationPreflightOriginAllowed(
        "https://unknown.example.com",
        requestUrl,
        ENV
      )
    ).toBe(false)
  })
})
