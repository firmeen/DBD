# TIPX Integration Contract v1

Issue: #55

TIPX exposes a versioned **launch integration boundary** for Central today and additional consuming web applications later. This boundary is separate from the Company, Spatial, Relationship and Analytics serving endpoints used by TIPX analytical surfaces.

## Endpoint

```text
POST /api/tipx/integration/v1/launch
```

The endpoint validates a launch request and returns a canonical TIPX deep link. It does not query MySQL or SQL Server and it does not grant access to TIPX data.

## Request

```json
{
  "version": "v1",
  "sourceApp": "central",
  "target": {
    "surface": "C",
    "entity": {
      "kind": "company",
      "id": "0100000000000",
      "labelHint": "Example Company"
    }
  },
  "lens": {
    "province": "Bangkok",
    "fiscalYear": 2025
  },
  "returnContext": {
    "path": "/companies/0100000000000",
    "label": "Back to Central"
  }
}
```

All objects are strict. Unknown fields are rejected deliberately.

### Security boundary

The launch payload **must not contain** user identities, roles, permission claims, access tokens, raw database records or DBD/Sale-Detail mapping data.

Authentication remains a session/upstream concern. Authorization remains enforced by TIPX and its serving layer. A valid launch URL never grants additional data access.

`sourceApp` is navigation provenance, not an authorization claim.

## Source application registry

Central is the default enabled source.

Additional web applications can use the same v1 contract by configuration:

```text
TIPX_INTEGRATION_SOURCES=central,sales,crm
```

For browser-to-browser integration, bind each source application to approved origins:

```text
TIPX_INTEGRATION_SOURCE_ORIGINS={"central":["https://central.example.com"],"sales":["https://sales.example.com"]}
```

Cross-origin browser requests are denied unless the request origin is configured for the declared source application. Same-origin and server-to-server requests do not require CORS configuration.

Set the canonical public TIPX URL when deployment origin cannot be derived from the incoming request:

```text
TIPX_PUBLIC_URL=https://tipx.example.com/
```

## Response

```json
{
  "ok": true,
  "version": "v1",
  "launch": {
    "href": "https://tipx.example.com/?tipx_source=central&tipx_surface=C&tipx_entity_kind=company&tipx_entity_id=0100000000000",
    "context": {}
  },
  "security": {
    "launchContextGrantsAccess": false,
    "authenticationBoundary": "session-or-upstream",
    "authorizationBoundary": "tipx-serving-layer"
  }
}
```

## TIPX bootstrap

When TIPX opens a generated deep link, the page validates the integration query again before hydrating the shared workspace.

The external launch can initialize:

- active analytical surface
- canonical focus entity
- Global Lens values
- source application
- safe application-relative return context

Selection, Compare, Population, surface snapshots and workspace history are cleared when an external launch context is hydrated so context from a previous investigation cannot leak into a new external launch.

## Return navigation

v1 accepts only an application-relative `returnContext.path`, for example:

```text
/companies/0100000000000
```

Absolute and protocol-relative return URLs are rejected. This avoids turning TIPX into an open redirect. The source application origin remains deployment configuration rather than caller-controlled data.

## Error contract

Errors use a stable envelope:

```json
{
  "ok": false,
  "version": "v1",
  "error": {
    "code": "invalid_request",
    "message": "Launch request does not match the TIPX integration v1 contract.",
    "issues": [
      {
        "path": "target.entity.id",
        "message": "String must contain at least 1 character(s)"
      }
    ]
  }
}
```

Defined error codes:

- `invalid_json`
- `invalid_request`
- `source_not_allowed`
- `origin_not_allowed`

## Data boundary

The integration contract never requires a consuming web application to:

- connect to MySQL or SQL Server
- query raw DBD tables
- reconstruct canonical company identity
- duplicate DBD ↔ Sale-Detail mapping
- duplicate TIPX serving business rules

The launch integration boundary and analytical serving endpoints remain separate concerns.
