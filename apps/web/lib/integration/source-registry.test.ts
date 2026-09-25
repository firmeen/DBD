import { describe, expect, it } from "vitest"

import {
  isIntegrationPreflightOriginAllowed,
  isIntegrationRequestOriginAllowed,
  isIntegrationSourceEnabled,
} from "./source-registry"

const ENV = {
  TIPX_INTEGRATION_SOURCES: "central,sales",
  TIPX_INTEGRATION_SOURCE_ORIGINS: JSON.stringify({
    central: ["https://central.example.com"],
    sales: ["https://sales.example.com"],
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
