export interface OwnershipComposition {
  thaiPct: number
  otherPct: number
  thaiWidth: number
  otherWidth: number
}

export function normalizeOwnershipComposition(
  thaiPct: number | null | undefined,
  otherPct: number | null | undefined
): OwnershipComposition | null {
  if (
    thaiPct === null ||
    thaiPct === undefined ||
    otherPct === null ||
    otherPct === undefined ||
    !Number.isFinite(thaiPct) ||
    !Number.isFinite(otherPct)
  ) {
    return null
  }

  const thai = Math.max(0, Math.min(100, thaiPct))
  const other = Math.max(0, Math.min(100, otherPct))
  const total = thai + other

  if (total <= 0) return null

  return {
    thaiPct: thai,
    otherPct: other,
    thaiWidth: (thai / total) * 100,
    otherWidth: (other / total) * 100,
  }
}
