export function aiAssistantEntryEnabled(
  value = process.env.NEXT_PUBLIC_TIPX_AI_ENTRY_ENABLED
) {
  return parseFeatureFlag(value, true)
}

export function aiAssistantAmbientMotionEnabled(
  value = process.env.NEXT_PUBLIC_TIPX_AI_AMBIENT_MOTION
) {
  return parseFeatureFlag(value, true)
}

function parseFeatureFlag(
  value: string | undefined,
  fallback: boolean
) {
  if (value == null || value.trim() === "") return fallback

  const normalized = value.trim().toLowerCase()
  if (["1", "true", "yes", "on"].includes(normalized)) return true
  if (["0", "false", "no", "off"].includes(normalized)) return false

  return fallback
}
