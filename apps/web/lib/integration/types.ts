import type {
  EntityKind,
  LensState,
  SurfaceId,
} from "@/lib/intelligence/types"

export const TIPX_INTEGRATION_VERSION = "v1" as const

export type TipxIntegrationVersion = typeof TIPX_INTEGRATION_VERSION

export interface IntegrationTargetEntity {
  kind: EntityKind
  id: string
  labelHint?: string
}

export interface IntegrationTarget {
  surface?: SurfaceId
  entity?: IntegrationTargetEntity
}

export type IntegrationLens = Partial<LensState>

export interface IntegrationReturnContext {
  path: string
  label?: string
}

export interface IntegrationLaunchRequest {
  version: TipxIntegrationVersion
  sourceApp: string
  target?: IntegrationTarget
  lens?: IntegrationLens
  returnContext?: IntegrationReturnContext
}

export interface IntegrationLaunchContext {
  version: TipxIntegrationVersion
  sourceApp: string
  target: {
    surface: SurfaceId
    entity?: IntegrationTargetEntity
  }
  lens?: IntegrationLens
  returnContext?: IntegrationReturnContext
}

export interface IntegrationLaunchSuccess {
  ok: true
  version: TipxIntegrationVersion
  launch: {
    href: string
    context: IntegrationLaunchContext
  }
  security: {
    launchContextGrantsAccess: false
    authenticationBoundary: "session-or-upstream"
    authorizationBoundary: "tipx-serving-layer"
  }
}

export interface IntegrationErrorIssue {
  path: string
  message: string
}

export interface IntegrationErrorResponse {
  ok: false
  version: TipxIntegrationVersion
  error: {
    code:
      | "invalid_json"
      | "invalid_request"
      | "source_not_allowed"
      | "origin_not_allowed"
    message: string
    issues?: IntegrationErrorIssue[]
  }
}
