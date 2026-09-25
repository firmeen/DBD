import { describe, expect, it } from "vitest"

import {
  AI_AMBIENT_BOUNDS,
  aiAmbientDelay,
  createAiAmbientTarget,
} from "./motion"

describe("TIPX AI ambient motion", () => {
  it("keeps roaming inside the bounded desktop envelope", () => {
    const low = createAiAmbientTarget(() => 0)
    const high = createAiAmbientTarget(() => 1)

    expect(low.x).toBe(AI_AMBIENT_BOUNDS.x[0])
    expect(low.y).toBe(AI_AMBIENT_BOUNDS.y[0])
    expect(low.rotate).toBe(AI_AMBIENT_BOUNDS.rotate[0])
    expect(low.duration).toBe(AI_AMBIENT_BOUNDS.duration[0])
    expect(low.restMs).toBe(AI_AMBIENT_BOUNDS.restMs[0])

    expect(high.x).toBe(AI_AMBIENT_BOUNDS.x[1])
    expect(high.y).toBe(AI_AMBIENT_BOUNDS.y[1])
    expect(high.rotate).toBe(AI_AMBIENT_BOUNDS.rotate[1])
    expect(high.duration).toBe(AI_AMBIENT_BOUNDS.duration[1])
    expect(high.restMs).toBe(AI_AMBIENT_BOUNDS.restMs[1])
  })

  it("schedules the next drift after travel and rest", () => {
    const target = createAiAmbientTarget(() => 0.5)

    expect(aiAmbientDelay(target)).toBe(
      Math.round(target.duration * 1_000 + target.restMs)
    )
  })
})
