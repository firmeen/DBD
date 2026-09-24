import { z } from "zod"

import {
  RELATIONSHIP_VIEW_IDS,
  type PeopleSurfaceSnapshot,
  type RelationshipSurfaceSnapshot,
} from "./types"

const peopleSnapshotSchema = z.object({
  focusedPersonId: z.string().min(1).nullable(),
  searchTerm: z.string().max(200),
})

const relationshipSnapshotSchema = z.object({
  view: z.enum(RELATIONSHIP_VIEW_IDS),
  depth: z.number().int().min(1).max(4),
  matrixCompanyQuery: z.string().max(200),
  matrixPersonQuery: z.string().max(200),
})

export function resolvePeopleSurfaceSnapshot(
  input: unknown,
  focusedPersonId: string | null
): PeopleSurfaceSnapshot {
  const parsed = peopleSnapshotSchema.safeParse(input)
  if (!parsed.success || parsed.data.focusedPersonId !== focusedPersonId) {
    return {
      focusedPersonId,
      searchTerm: "",
    }
  }
  return parsed.data
}

export function defaultRelationshipSurfaceSnapshot(): RelationshipSurfaceSnapshot {
  return {
    view: "explore",
    depth: 1,
    matrixCompanyQuery: "",
    matrixPersonQuery: "",
  }
}

export function parseRelationshipSurfaceSnapshot(
  input: unknown
): RelationshipSurfaceSnapshot {
  const parsed = relationshipSnapshotSchema.safeParse(input)
  return parsed.success
    ? parsed.data
    : defaultRelationshipSurfaceSnapshot()
}
