import { z } from "zod"

import { SURFACE_IDS } from "@/lib/intelligence/types"
import {
  TIPX_INTEGRATION_VERSION,
  type IntegrationLaunchRequest,
} from "@/lib/integration/types"

const sourceAppSchema = z
  .string()
  .trim()
  .min(1)
  .max(48)
  .regex(
    /^[a-z][a-z0-9-]*$/,
    "sourceApp must be a lowercase application slug"
  )

const boundedContextText = z.string().trim().min(1).max(191)
const surfaceSchema = z.enum(SURFACE_IDS)
const entityKindSchema = z.enum(["company", "person", "area", "branch"])

const entitySchema = z
  .object({
    kind: entityKindSchema,
    id: z.string().trim().min(1).max(128),
    labelHint: z.string().trim().min(1).max(500).optional(),
  })
  .strict()

const targetSchema = z
  .object({
    surface: surfaceSchema.optional(),
    entity: entitySchema.optional(),
  })
  .strict()

const lensSchema = z
  .object({
    province: boundedContextText.nullable().optional(),
    district: boundedContextText.nullable().optional(),
    business: boundedContextText.nullable().optional(),
    tipStatus: z
      .enum(["active", "expired", "prospect"])
      .nullable()
      .optional(),
    fiscalYear: z.number().int().min(1900).max(3000).nullable().optional(),
  })
  .strict()

const returnPathSchema = z
  .string()
  .trim()
  .min(1)
  .max(2_048)
  .refine(isSafeRelativeReturnPath, {
    message: "returnContext.path must be a safe application-relative path",
  })

const returnContextSchema = z
  .object({
    path: returnPathSchema,
    label: z.string().trim().min(1).max(120).optional(),
  })
  .strict()

const launchRequestSchema = z
  .object({
    version: z.literal(TIPX_INTEGRATION_VERSION),
    sourceApp: sourceAppSchema,
    target: targetSchema.optional(),
    lens: lensSchema.optional(),
    returnContext: returnContextSchema.optional(),
  })
  .strict()

export function parseIntegrationLaunchRequest(
  input: unknown
): IntegrationLaunchRequest {
  return launchRequestSchema.parse(input) as IntegrationLaunchRequest
}

export function isSafeRelativeReturnPath(value: string) {
  if (
    !value.startsWith("/") ||
    value.startsWith("//") ||
    value.includes("\\") ||
    /[\u0000-\u001f\u007f]/.test(value)
  ) {
    return false
  }

  try {
    const base = new URL("https://tipx.invalid")
    const parsed = new URL(value, base)

    return parsed.origin === base.origin
  } catch {
    return false
  }
}
