import { isSafeRelativeReturnPath } from "@/lib/integration/contract"
import {
  isIntegrationSourceEnabled,
  resolveIntegrationReturnUrl,
} from "@/lib/integration/source-registry"

export const runtime = "nodejs"

export function GET(request: Request) {
  const url = new URL(request.url)
  const sourceApp = url.searchParams.get("source")
  const path = url.searchParams.get("path")

  if (!sourceApp || !path || !isSafeRelativeReturnPath(path)) {
    return Response.json(
      {
        ok: false,
        error: {
          code: "invalid_return_context",
          message: "Return navigation context is invalid.",
        },
      },
      { status: 400 }
    )
  }

  if (!isIntegrationSourceEnabled(sourceApp)) {
    return Response.json(
      {
        ok: false,
        error: {
          code: "source_not_allowed",
          message: "The requested source application is not enabled.",
        },
      },
      { status: 403 }
    )
  }

  const destination = resolveIntegrationReturnUrl(sourceApp, path)
  if (!destination) {
    return Response.json(
      {
        ok: false,
        error: {
          code: "return_not_configured",
          message:
            "Return navigation is not configured for this source application.",
        },
      },
      { status: 409 }
    )
  }

  return new Response(null, {
    status: 302,
    headers: {
      Location: destination,
      "Cache-Control": "no-store",
    },
  })
}
