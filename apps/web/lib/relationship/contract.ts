import { z } from "zod"

import {
  MAX_GENEALOGY_EDGES,
  MAX_GENEALOGY_NODES,
  MAX_MATRIX_COMPANIES,
  MAX_MATRIX_MEMBERSHIPS,
  MAX_MATRIX_PEOPLE,
  MAX_PERSON_COMPANIES,
  MAX_PERSON_SEARCH_RESULTS,
  MAX_RELATIONSHIP_EDGES,
  MAX_RELATIONSHIP_NODES,
  MAX_SHARED_DIRECTORS,
  type BoardMatrixSnapshot,
  type CompanyBoardSnapshot,
  type GenealogySnapshot,
  type PersonDetailSnapshot,
  type PersonSearchSnapshot,
  type RelationshipGraphSnapshot,
} from "./types"

const MAX_TEXT = 8_000
const MAX_MESSAGE = 2_000
const MAX_LABEL = 500
const MAX_SECONDARY = 1_000

const boundedText = z.string().max(MAX_TEXT)
const nullableText = boundedText.nullable().optional()
const messageText = z.string().max(MAX_MESSAGE).optional()
const dateText = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .nullable()
  .optional()
const nullableFinite = z.number().finite().nullable().optional()

const companyEntitySchema = z.object({
  id: z.string().min(1).max(64),
  kind: z.literal("company"),
  label: z.string().min(1).max(MAX_LABEL),
  secondary: z.string().max(MAX_SECONDARY).optional(),
})

const personEntitySchema = z.object({
  id: z.string().min(1).max(128),
  kind: z.literal("person"),
  label: z.string().min(1).max(MAX_LABEL),
  secondary: z.string().max(MAX_SECONDARY).optional(),
})

const personSearchResultSchema = z.object({
  id: z.string().min(1).max(128),
  name: z.string().min(1).max(MAX_LABEL),
  secondary: z.string().max(MAX_SECONDARY).nullable().optional(),
  companyCount: z.number().int().nonnegative().nullable(),
  activeCompanyCount: z.number().int().nonnegative().nullable().optional(),
  tipCompanyCount: z.number().int().nonnegative().nullable().optional(),
})

const personSearchSchema = z.object({
  availability: z.enum(["ready", "partial", "unavailable"]),
  message: messageText,
  generatedAt: nullableText,
  results: z
    .array(personSearchResultSchema)
    .max(MAX_PERSON_SEARCH_RESULTS),
})

const personProfileSchema = personSearchResultSchema

const personCompanyRoleSchema = z.object({
  company: companyEntitySchema,
  role: nullableText,
  appointedAt: dateText,
  endedAt: dateText,
  companyStatus: nullableText,
  tipStatus: z.enum(["active", "expired", "prospect"]).nullable().optional(),
  latestFiscalYear: z.string().max(8).nullable().optional(),
  totalIncome: nullableFinite,
  netProfit: nullableFinite,
})

const sharedDirectorSchema = z.object({
  person: personEntitySchema,
  sharedCompanyCount: z.number().int().nonnegative(),
  companies: z.array(companyEntitySchema).max(128),
})

const personDetailSchema = z.object({
  availability: z.enum(["ready", "partial", "unavailable"]),
  message: messageText,
  generatedAt: nullableText,
  profile: personProfileSchema.nullable(),
  companies: z.array(personCompanyRoleSchema).max(MAX_PERSON_COMPANIES),
  sharedDirectors: z
    .array(sharedDirectorSchema)
    .max(MAX_SHARED_DIRECTORS),
})

const companyBoardSchema = z.object({
  availability: z.enum(["ready", "partial", "unavailable"]),
  message: messageText,
  generatedAt: nullableText,
  company: companyEntitySchema.nullable(),
  members: z
    .array(
      z.object({
        person: personEntitySchema,
        role: nullableText,
        appointedAt: dateText,
        endedAt: dateText,
        sharedCompanyCount: z
          .number()
          .int()
          .nonnegative()
          .nullable()
          .optional(),
      })
    )
    .max(MAX_PERSON_COMPANIES),
})

const relationshipNodeSchema = z.object({
  id: z.string().min(1).max(128),
  kind: z.enum(["company", "person"]),
  label: z.string().min(1).max(MAX_LABEL),
  secondary: z.string().max(MAX_SECONDARY).nullable().optional(),
  degree: z.number().int().nonnegative(),
  tipStatus: z.enum(["active", "expired", "prospect"]).nullable().optional(),
  x: nullableFinite,
  y: nullableFinite,
})

const relationshipEdgeSchema = z.object({
  id: z.string().min(1).max(192),
  source: z.string().min(1).max(128),
  target: z.string().min(1).max(128),
  kind: z.enum(["board", "shared-director", "transform", "merger"]),
  label: nullableText,
  weight: nullableFinite,
  directed: z.boolean().optional(),
})

const relationshipGraphSchema = z
  .object({
    availability: z.enum(["ready", "partial", "unavailable"]),
    message: messageText,
    generatedAt: nullableText,
    layout: z.enum(["server", "client"]),
    nodes: z.array(relationshipNodeSchema).max(MAX_RELATIONSHIP_NODES),
    edges: z.array(relationshipEdgeSchema).max(MAX_RELATIONSHIP_EDGES),
  })
  .superRefine((snapshot, context) => {
    const nodeIds = new Set(snapshot.nodes.map((node) => node.id))
    const edgeIds = new Set<string>()

    if (nodeIds.size !== snapshot.nodes.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["nodes"],
        message: "Relationship graph contains duplicate node IDs",
      })
    }

    if (snapshot.layout === "server") {
      for (const [index, node] of snapshot.nodes.entries()) {
        if (node.x == null || node.y == null) {
          context.addIssue({
            code: z.ZodIssueCode.custom,
            path: ["nodes", index],
            message:
              "Server-layout relationship nodes require finite x/y coordinates",
          })
        }
      }
    }

    for (const [index, edge] of snapshot.edges.entries()) {
      if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["edges", index],
          message: "Relationship edge references a missing node",
        })
      }

      if (edgeIds.has(edge.id)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["edges", index, "id"],
          message: "Relationship graph contains duplicate edge IDs",
        })
      }
      edgeIds.add(edge.id)
    }
  })

const matrixCompanySchema = z.object({
  id: z.string().min(1).max(64),
  name: z.string().min(1).max(MAX_LABEL),
  secondary: z.string().max(MAX_SECONDARY).nullable().optional(),
  degree: z.number().int().nonnegative(),
  tipStatus: z.enum(["active", "expired", "prospect"]).nullable().optional(),
})

const matrixPersonSchema = z.object({
  id: z.string().min(1).max(128),
  name: z.string().min(1).max(MAX_LABEL),
  secondary: z.string().max(MAX_SECONDARY).nullable().optional(),
  degree: z.number().int().nonnegative(),
})

const boardMatrixSchema = z
  .object({
    availability: z.enum(["ready", "partial", "unavailable"]),
    message: messageText,
    generatedAt: nullableText,
    companies: z.array(matrixCompanySchema).max(MAX_MATRIX_COMPANIES),
    people: z.array(matrixPersonSchema).max(MAX_MATRIX_PEOPLE),
    memberships: z
      .array(
        z.object({
          companyId: z.string().min(1).max(64),
          personId: z.string().min(1).max(128),
          role: nullableText,
        })
      )
      .max(MAX_MATRIX_MEMBERSHIPS),
  })
  .superRefine((snapshot, context) => {
    const companies = new Set(snapshot.companies.map((company) => company.id))
    const people = new Set(snapshot.people.map((person) => person.id))
    const membershipKeys = new Set<string>()

    if (companies.size !== snapshot.companies.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["companies"],
        message: "Board Matrix contains duplicate company axis IDs",
      })
    }

    if (people.size !== snapshot.people.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["people"],
        message: "Board Matrix contains duplicate person axis IDs",
      })
    }

    for (const [index, membership] of snapshot.memberships.entries()) {
      if (!companies.has(membership.companyId) || !people.has(membership.personId)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["memberships", index],
          message: "Board Matrix membership references a missing axis entity",
        })
      }

      const key = `${membership.companyId}::${membership.personId}`
      if (membershipKeys.has(key)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["memberships", index],
          message: "Board Matrix contains duplicate membership cells",
        })
      }
      membershipKeys.add(key)
    }
  })

const genealogySchema = z
  .object({
    availability: z.enum(["ready", "partial", "unavailable"]),
    message: messageText,
    generatedAt: nullableText,
    nodes: z
      .array(
        z.object({
          id: z.string().min(1).max(128),
          company: companyEntitySchema,
        })
      )
      .max(MAX_GENEALOGY_NODES),
    edges: z
      .array(
        z.object({
          id: z.string().min(1).max(192),
          source: z.string().min(1).max(128),
          target: z.string().min(1).max(128),
          kind: z.enum(["transform", "merger"]),
          date: dateText,
          label: nullableText,
        })
      )
      .max(MAX_GENEALOGY_EDGES),
  })
  .superRefine((snapshot, context) => {
    const nodeIds = new Set(snapshot.nodes.map((node) => node.id))
    const edgeIds = new Set<string>()

    if (nodeIds.size !== snapshot.nodes.length) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["nodes"],
        message: "Genealogy contains duplicate node IDs",
      })
    }

    for (const [index, edge] of snapshot.edges.entries()) {
      if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["edges", index],
          message: "Genealogy edge references a missing company node",
        })
      }

      if (edgeIds.has(edge.id)) {
        context.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["edges", index, "id"],
          message: "Genealogy contains duplicate edge IDs",
        })
      }
      edgeIds.add(edge.id)
    }
  })

export function parsePersonSearchSnapshot(input: unknown): PersonSearchSnapshot {
  return personSearchSchema.parse(input) as PersonSearchSnapshot
}

export function parsePersonDetailSnapshot(
  input: unknown,
  expectedPersonId?: string
): PersonDetailSnapshot {
  const snapshot = personDetailSchema.parse(input) as PersonDetailSnapshot
  if (
    expectedPersonId &&
    snapshot.profile &&
    snapshot.profile.id !== expectedPersonId
  ) {
    throw new Error(
      `Person detail response identity mismatch: expected ${expectedPersonId}, received ${snapshot.profile.id}`
    )
  }

  if (
    snapshot.profile === null &&
    (snapshot.availability !== "unavailable" ||
      snapshot.companies.length > 0 ||
      snapshot.sharedDirectors.length > 0)
  ) {
    throw new Error(
      "Person detail response without a bound profile must be an empty unavailable payload"
    )
  }

  return snapshot
}

export function parseCompanyBoardSnapshot(
  input: unknown,
  expectedCompanyId?: string
): CompanyBoardSnapshot {
  const snapshot = companyBoardSchema.parse(input) as CompanyBoardSnapshot
  if (
    expectedCompanyId &&
    snapshot.company &&
    snapshot.company.id !== expectedCompanyId
  ) {
    throw new Error(
      `Company board response identity mismatch: expected ${expectedCompanyId}, received ${snapshot.company.id}`
    )
  }

  if (
    snapshot.company === null &&
    (snapshot.availability !== "unavailable" || snapshot.members.length > 0)
  ) {
    throw new Error(
      "Company board response without a bound company must be an empty unavailable payload"
    )
  }

  return snapshot
}

export function parseRelationshipGraphSnapshot(
  input: unknown
): RelationshipGraphSnapshot {
  return relationshipGraphSchema.parse(input) as RelationshipGraphSnapshot
}

export function parseBoardMatrixSnapshot(input: unknown): BoardMatrixSnapshot {
  return boardMatrixSchema.parse(input) as BoardMatrixSnapshot
}

export function parseGenealogySnapshot(input: unknown): GenealogySnapshot {
  return genealogySchema.parse(input) as GenealogySnapshot
}
