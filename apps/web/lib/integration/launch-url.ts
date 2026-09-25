import { parseIntegrationLaunchRequest } from "./contract"
import {
  TIPX_INTEGRATION_VERSION,
  type IntegrationLaunchContext,
  type IntegrationLaunchRequest,
} from "./types"
import type {
  EntityKind,
  SurfaceId,
} from "@/lib/intelligence/types"

type SearchParamsRecord = Record<
  string,
  string | string[] | undefined
>

export function inferSurfaceForEntity(kind?: EntityKind): SurfaceId {
  if (kind === "company") return "C"
  if (kind === "person") return "P"
  return "M"
}

export function normalizeIntegrationLaunchRequest(
  request: IntegrationLaunchRequest
): IntegrationLaunchContext {
  const entity = request.target?.entity
  const surface =
    request.target?.surface ?? inferSurfaceForEntity(entity?.kind)

  return {
    version: TIPX_INTEGRATION_VERSION,
    sourceApp: request.sourceApp,
    target: {
      surface,
      ...(entity ? { entity } : {}),
    },
    ...(request.lens ? { lens: request.lens } : {}),
    ...(request.returnContext
      ? { returnContext: request.returnContext }
      : {}),
  }
}

export function buildTipxLaunchHref(
  context: IntegrationLaunchContext,
  publicUrl: string
) {
  const url = new URL(publicUrl)
  url.search = ""
  url.hash = ""

  url.searchParams.set("tipx_source", context.sourceApp)
  url.searchParams.set("tipx_surface", context.target.surface)

  const entity = context.target.entity
  if (entity) {
    url.searchParams.set("tipx_entity_kind", entity.kind)
    url.searchParams.set("tipx_entity_id", entity.id)
    if (entity.labelHint) {
      url.searchParams.set("tipx_entity_label", entity.labelHint)
    }
  }

  const lens = context.lens
  if (lens) {
    setNullableSearchParam(url, "province", lens.province)
    setNullableSearchParam(url, "district", lens.district)
    setNullableSearchParam(url, "business", lens.business)
    setNullableSearchParam(url, "tipStatus", lens.tipStatus)
    if (lens.fiscalYear != null) {
      url.searchParams.set("fiscalYear", String(lens.fiscalYear))
    }
  }

  if (context.returnContext) {
    url.searchParams.set("tipx_return_path", context.returnContext.path)
    if (context.returnContext.label) {
      url.searchParams.set(
        "tipx_return_label",
        context.returnContext.label
      )
    }
  }

  return url.toString()
}

export function parseTipxIntegrationSearchParams(
  params: SearchParamsRecord
): IntegrationLaunchContext | null {
  const sourceApp = single(params.tipx_source)
  if (!sourceApp) return null

  const surface = single(params.tipx_surface)
  const entityKind = single(params.tipx_entity_kind)
  const entityId = single(params.tipx_entity_id)
  const entityLabel = single(params.tipx_entity_label)
  const returnPath = single(params.tipx_return_path)
  const returnLabel = single(params.tipx_return_label)

  const fiscalYearRaw = single(params.fiscalYear)
  const fiscalYear =
    fiscalYearRaw == null || fiscalYearRaw === ""
      ? undefined
      : Number(fiscalYearRaw)

  const target =
    surface || (entityKind && entityId)
      ? {
          ...(surface ? { surface } : {}),
          ...(entityKind && entityId
            ? {
                entity: {
                  kind: entityKind,
                  id: entityId,
                  ...(entityLabel ? { labelHint: entityLabel } : {}),
                },
              }
            : {}),
        }
      : undefined

  const lens = compactObject({
    province: single(params.province),
    district: single(params.district),
    business: single(params.business),
    tipStatus: single(params.tipStatus),
    fiscalYear,
  })

  const candidate = {
    version: TIPX_INTEGRATION_VERSION,
    sourceApp,
    ...(target ? { target } : {}),
    ...(Object.keys(lens).length > 0 ? { lens } : {}),
    ...(returnPath
      ? {
          returnContext: {
            path: returnPath,
            ...(returnLabel ? { label: returnLabel } : {}),
          },
        }
      : {}),
  }

  try {
    return normalizeIntegrationLaunchRequest(
      parseIntegrationLaunchRequest(candidate)
    )
  } catch {
    return null
  }
}

function setNullableSearchParam(
  url: URL,
  key: string,
  value: string | null | undefined
) {
  if (value) {
    url.searchParams.set(key, value)
  }
}

function single(value: string | string[] | undefined) {
  if (Array.isArray(value)) return value[0]
  return value
}

function compactObject(
  value: Record<string, string | number | undefined>
) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined)
  )
}
