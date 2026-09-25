export interface AiAmbientTarget {
  x: number
  y: number
  rotate: number
  duration: number
  restMs: number
}

export const AI_AMBIENT_BOUNDS = {
  x: [-20, 28],
  y: [-36, 36],
  rotate: [-3.5, 3.5],
  duration: [7.5, 13.5],
  restMs: [2_600, 6_800],
} as const

export function createAiAmbientTarget(
  random: () => number = Math.random
): AiAmbientTarget {
  return {
    x: between(AI_AMBIENT_BOUNDS.x, random()),
    y: between(AI_AMBIENT_BOUNDS.y, random()),
    rotate: between(AI_AMBIENT_BOUNDS.rotate, random()),
    duration: between(AI_AMBIENT_BOUNDS.duration, random()),
    restMs: Math.round(
      between(AI_AMBIENT_BOUNDS.restMs, random())
    ),
  }
}

export function aiAmbientDelay(target: AiAmbientTarget) {
  return Math.round(target.duration * 1_000 + target.restMs)
}

function between(
  range: readonly [number, number],
  sample: number
) {
  const bounded = Math.min(1, Math.max(0, sample))
  return range[0] + (range[1] - range[0]) * bounded
}
