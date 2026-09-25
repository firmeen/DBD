type Environment = Record<string, string | undefined>

const DEFAULT_SOURCE_APPS = ["central"] as const

export function isIntegrationSourceEnabled(
  sourceApp: string,
  env: Environment = process.env
) {
  return configuredSourceApps(env).has(sourceApp)
}

export function isIntegrationRequestOriginAllowed(
  sourceApp: string,
  origin: string | null,
  requestUrl: string,
  env: Environment = process.env
) {
  if (!origin) return true

  const requestOrigin = new URL(requestUrl).origin
  if (normalizeOrigin(origin) === requestOrigin) return true

  const configured = configuredSourceOrigins(env)
  return configured[sourceApp]?.has(normalizeOrigin(origin)) ?? false
}

export function isIntegrationPreflightOriginAllowed(
  origin: string | null,
  requestUrl: string,
  env: Environment = process.env
) {
  if (!origin) return true

  const normalized = normalizeOrigin(origin)
  if (normalized === new URL(requestUrl).origin) return true

  return Object.values(configuredSourceOrigins(env)).some((origins) =>
    origins.has(normalized)
  )
}

export function integrationCorsHeaders(origin: string | null) {
  if (!origin) return {}

  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "content-type",
    "Access-Control-Max-Age": "600",
    Vary: "Origin",
  }
}

export function resolveIntegrationReturnUrl(
  sourceApp: string,
  path: string,
  env: Environment = process.env
) {
  if (
    !isIntegrationSourceEnabled(sourceApp, env) ||
    !path.startsWith("/") ||
    path.startsWith("//") ||
    path.includes("\\")
  ) {
    return null
  }

  const explicitOrigin = configuredReturnOrigins(env)[sourceApp]
  const sourceOrigins = configuredSourceOrigins(env)[sourceApp]
  const fallbackOrigin =
    sourceOrigins?.size === 1 ? [...sourceOrigins][0] : undefined
  const origin = explicitOrigin ?? fallbackOrigin

  if (!origin) return null

  try {
    const destination = new URL(path, origin)
    return destination.origin === origin
      ? destination.toString()
      : null
  } catch {
    return null
  }
}

function configuredSourceApps(env: Environment) {
  const configured = env.TIPX_INTEGRATION_SOURCES?.split(",")
    .map((value) => value.trim())
    .filter(Boolean)

  return new Set(
    configured && configured.length > 0
      ? configured
      : DEFAULT_SOURCE_APPS
  )
}

function configuredReturnOrigins(env: Environment) {
  const raw = env.TIPX_INTEGRATION_RETURN_ORIGINS?.trim()
  if (!raw) return {} as Record<string, string>

  try {
    const parsed = JSON.parse(raw) as unknown
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {}
    }

    const result: Record<string, string> = {}
    for (const [source, value] of Object.entries(parsed)) {
      if (typeof value !== "string") continue
      const origin = normalizeOrigin(value)
      if (origin) result[source] = origin
    }

    return result
  } catch {
    return {}
  }
}

function configuredSourceOrigins(env: Environment) {
  const raw = env.TIPX_INTEGRATION_SOURCE_ORIGINS?.trim()
  if (!raw) return {} as Record<string, Set<string>>

  try {
    const parsed = JSON.parse(raw) as unknown
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {}
    }

    const result: Record<string, Set<string>> = {}

    for (const [source, value] of Object.entries(parsed)) {
      if (!Array.isArray(value)) continue

      const origins = value
        .filter((entry): entry is string => typeof entry === "string")
        .map(normalizeOrigin)
        .filter(Boolean)

      result[source] = new Set(origins)
    }

    return result
  } catch {
    return {}
  }
}

function normalizeOrigin(value: string) {
  try {
    return new URL(value).origin
  } catch {
    return ""
  }
}
