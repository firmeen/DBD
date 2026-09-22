import { z } from "zod"

import type { SpatialSnapshot } from "./types"

const coordinateSchema = z.tuple([z.number(), z.number()])

const companySchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  coordinates: coordinateSchema,
  businessType: z.string().nullable().optional(),
  province: z.string().nullable().optional(),
  district: z.string().nullable().optional(),
  revenue: z.number().finite().nullable().optional(),
  tipStatus: z.enum(["active", "expired", "prospect"]).nullable().optional(),
  boardCount: z.number().int().nonnegative().nullable().optional(),
  logoUrl: z.string().url().nullable().optional(),
})

const branchSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  coordinates: coordinateSchema,
  address: z.string().nullable().optional(),
})

const h3Schema = z.object({
  index: z.string().min(1),
  count: z.number().int().nonnegative(),
  active: z.number().int().nonnegative(),
  expired: z.number().int().nonnegative(),
  prospect: z.number().int().nonnegative(),
  opportunityScore: z.number().min(0).max(1).nullable().optional(),
})

const polygonCoordinatesSchema = z.array(z.array(coordinateSchema))
const multiPolygonCoordinatesSchema = z.array(polygonCoordinatesSchema)

const areaSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  level: z.enum(["province", "district"]),
  geometry: z.discriminatedUnion("type", [
    z.object({
      type: z.literal("Polygon"),
      coordinates: polygonCoordinatesSchema,
    }),
    z.object({
      type: z.literal("MultiPolygon"),
      coordinates: multiPolygonCoordinatesSchema,
    }),
  ]),
  companyCount: z.number().int().nonnegative().nullable().optional(),
  tipPenetration: z.number().min(0).max(1).nullable().optional(),
})

const snapshotSchema = z.object({
  availability: z.enum(["ready", "partial", "unavailable"]),
  message: z.string().optional(),
  generatedAt: z.string().nullable().optional(),
  capabilities: z.object({
    administrativeGeometry: z.boolean(),
    preciseCompanyGeometry: z.boolean(),
    branches: z.boolean(),
    h3Aggregates: z.boolean(),
    opportunityOverlay: z.boolean(),
  }),
  totals: z.object({
    companies: z.number().int().nonnegative().nullable(),
    active: z.number().int().nonnegative().nullable(),
    expired: z.number().int().nonnegative().nullable(),
    prospect: z.number().int().nonnegative().nullable(),
  }),
  companies: z.array(companySchema),
  branches: z.array(branchSchema),
  h3Cells: z.array(h3Schema),
  areas: z.array(areaSchema),
})

export function parseSpatialSnapshot(input: unknown): SpatialSnapshot {
  return snapshotSchema.parse(input) as SpatialSnapshot
}
