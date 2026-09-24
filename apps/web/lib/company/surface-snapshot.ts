import { z } from "zod"

import { COMPANY_VIEW_IDS, type CompanySurfaceSnapshot } from "./types"

const surfaceSnapshotSchema = z.object({
  focusedCompanyId: z.string().min(1).nullable().default(null),
  view: z.enum(COMPANY_VIEW_IDS),
  searchTerm: z.string().max(200),
  financialYear: z.string().max(8).nullable(),
})

export function parseCompanySurfaceSnapshot(
  input: unknown
): CompanySurfaceSnapshot | null {
  const parsed = surfaceSnapshotSchema.safeParse(input)
  return parsed.success ? parsed.data : null
}

export function resolveCompanySurfaceSnapshot(
  input: unknown,
  focusedCompanyId: string | null
): CompanySurfaceSnapshot {
  const snapshot = parseCompanySurfaceSnapshot(input)

  if (!snapshot || snapshot.focusedCompanyId !== focusedCompanyId) {
    return {
      ...defaultCompanySurfaceSnapshot(),
      focusedCompanyId,
    }
  }

  return snapshot
}

export function defaultCompanySurfaceSnapshot(): CompanySurfaceSnapshot {
  return {
    focusedCompanyId: null,
    view: "dossier",
    searchTerm: "",
    financialYear: null,
  }
}
