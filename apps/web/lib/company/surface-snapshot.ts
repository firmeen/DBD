import { z } from "zod"

import { COMPANY_VIEW_IDS, type CompanySurfaceSnapshot } from "./types"

const surfaceSnapshotSchema = z.object({
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

export function defaultCompanySurfaceSnapshot(): CompanySurfaceSnapshot {
  return {
    view: "dossier",
    searchTerm: "",
    financialYear: null,
  }
}
