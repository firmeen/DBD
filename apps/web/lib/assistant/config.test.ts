import { describe, expect, it } from "vitest"

import {
  aiAssistantAmbientMotionEnabled,
  aiAssistantEntryEnabled,
} from "./config"

describe("TIPX AI assistant feature flags", () => {
  it("shows the placeholder entry by default", () => {
    expect(aiAssistantEntryEnabled(undefined)).toBe(true)
  })

  it("can disable entry and ambient motion independently", () => {
    expect(aiAssistantEntryEnabled("false")).toBe(false)
    expect(aiAssistantAmbientMotionEnabled("off")).toBe(false)
    expect(aiAssistantEntryEnabled("true")).toBe(true)
    expect(aiAssistantAmbientMotionEnabled("on")).toBe(true)
  })

  it("falls back safely for unknown values", () => {
    expect(aiAssistantEntryEnabled("unexpected")).toBe(true)
  })
})
