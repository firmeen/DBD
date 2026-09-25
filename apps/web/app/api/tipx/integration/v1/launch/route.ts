import { ZodError } from "zod"

import { parseIntegrationLaunchRequest } from "@/lib/integration/contract"
import {
  buildTipxLaunchHref,
  normalizeIntegrationLaunchRequest,
} from "@/lib/integration/launch-url"
import {
  integrationCorsHeaders,
  isIntegrationPreflightOriginAllowed,
  isIntegrationRequestOriginAllowed,
  isIntegrationSourceEnabled,
} from "@/lib/integration/source-registry"
import {
  TIPX_INTEGRATION_VERSION,
  type IntegrationErrorResponse,
  type IntegrationLaunchSuccess,
} from "@/lib/integration/types"

export const runtime = "nodejs"

export async function POST(request: Request) {
  const origin = request.headers.get("origin")
  const responseHeaders = integrationCorsHeaders(origin)

  let input: unknown
  try {
    input = await request.json()
  } catch {
    return errorResponse(
      "invalid_json",
      "Request body must be valid JSON.",
      400,
      responseHeaders
    )
  }

  let launchRequest
  try {
    launchRequest = parseIntegrationLaunchRequest(input)
  } catch (error) {
    const issues =
      error instanceof ZodError
        ? error.issues.map((issue) => ({
            path: issue.path.join("."),
            message: issue.message,
          }))
        : undefined

    return errorResponse(
      "invalid_request",
      "Launch request does not match the TIPX integration v1 contract.",
      400,
      responseHeaders,
      issues
    )
  }

  if (!isIntegrationSourceEnabled(launchRequest.sourceApp)) {
    return errorResponse(
      "source_not_allowed",
      "The requested source application is not enabled for TIPX integration.",
      403,
      responseHeaders
    )
  }

  if (
    !isIntegrationRequestOriginAllowed(
      launchRequest.sourceApp,
      origin,
      request.url
    )
  ) {
    return errorResponse(
      "origin_not_allowed",
      "The browser origin is not allowed for the requested source application.",
      403,
      {}
    )
  }

  const context = normalizeIntegrationLaunchRequest(launchRequest)
  const publicUrl =
    process.env.TIPX_PUBLIC_URL?.trim() ||
    new URL("/", request.url).toString()

  const response: IntegrationLaunchSuccess = {
    ok: true,
    version: TIPX_INTEGRATION_VERSION,
    launch: {
      href: buildTipxLaunchHref(context, publicUrl),
      context,
    },
    security: {
      launchContextGrantsAccess: false,
      authenticationBoundary: "session-or-upstream",
      authorizationBoundary: "tipx-serving-layer",
    },
  }

  return Response.json(response, {
    status: 200,
    headers: responseHeaders,
  })
}

export async function OPTIONS(request: Request) {
  const origin = request.headers.get("origin")

  if (!isIntegrationPreflightOriginAllowed(origin, request.url)) {
    return new Response(null, { status: 403 })
  }

  return new Response(null, {
    status: 204,
    headers: integrationCorsHeaders(origin),
  })
}

function errorResponse(
  code: IntegrationErrorResponse["error"]["code"],
  message: string,
  status: number,
  headers: HeadersInit,
  issues?: IntegrationErrorResponse["error"]["issues"]
) {
  const body: IntegrationErrorResponse = {
    ok: false,
    version: TIPX_INTEGRATION_VERSION,
    error: {
      code,
      message,
      ...(issues && issues.length > 0 ? { issues } : {}),
    },
  }

  return Response.json(body, { status, headers })
}
