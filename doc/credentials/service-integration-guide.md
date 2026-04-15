# Credential Store — Service Integration Guide

This document explains how Go services consume credentials stored in ns_server's credential store via cbauth.
For the REST API see [rest-api-reference.md](rest-api-reference.md).
For credential type schemas see [credential-types.md](credential-types.md).

## Key Concept — `specialUser`

Every Go service connected to ns_server via cbauth receives a `specialUser` (e.g. `@cbq-engine`, `@backup`) and one or more `specialPasswords` through the revrpc Cache.
This is the service's own internal identity.
When cbauth calls ns_server endpoints (`/_cbauth/*`), it authenticates using `specialUser`/password via HTTP Basic Auth.
That identity satisfies the route-level `{[admin, internal], all}` guard.

`specialUser` is **not** a human admin.
It is the process-level identity ns_server assigns to each service at startup.

## Consumption Patterns

### Pattern A: Service consumes on behalf of an end user

A human user (e.g. `alice` in `local` domain) issues a N1QL query that needs an S3 credential.
The query service already authenticated `alice` and holds a `Creds` object for her.

```mermaid
sequenceDiagram
    participant A as alice (end user)
    participant Q as Query Service<br/>(@cbq-engine)
    participant N as ns_server

    A->>Q: N1QL: SELECT … FROM s3://…
    Note over Q: Already authenticated alice<br/>via cbauth. Holds Creds{<br/>  name: "alice",<br/>  domain: "local"<br/>}

    Q->>N: GET /_cbauth/getCredential/my-s3<br/>Auth: @cbq-engine (Basic)<br/>?user=alice&domain=local

    Note over N: Service = n1ql (from @cbq-engine)
    Note over N: On-behalf-of = alice/local (end user)
    Note over N: ✓ Known service
    Note over N: ✓ alice has consume permission
    Note over N: ✓ n1ql in allowedServices

    N-->>Q: 200 {type:"aws", fields:{accessKeyId:…, …}}
    Q->>Q: Use credential for S3 access
```

### Pattern B: Service consumes as itself

Continuous backup (`@cbcontbk`) runs a backup job and needs cloud credentials.
It authenticates using its `specialUser` and password (obtained via `cbauth.GetHTTPServiceAuth`), and consumes the credential directly as its own service identity.

`@cbcontbk` is an auth alias that maps to the canonical service identity `@backup` (see `misc:canonical_admin_identity/1`).
The RBAC permission check resolves the alias to `@backup` and looks up `credential_consumer` roles for that canonical identity.

```mermaid
sequenceDiagram
    participant B as Continuous Backup<br/>(@cbcontbk)
    participant N as ns_server

    Note over B: specialUser = @cbcontbk<br/>(alias for @backup)
    Note over B: Creds{<br/>  name: "@cbcontbk",<br/>  domain: "admin"<br/>}

    B->>N: GET /_cbauth/getCredential/cloud-backup<br/>Auth: @cbcontbk (Basic)<br/>?user=@cbcontbk&domain=admin

    Note over N: Service = backup<br/>(from @cbcontbk via<br/>identity_name_to_service)
    Note over N: On-behalf-of = @cbcontbk/admin<br/>(service identity, canonical = @backup)
    Note over N: ✓ Matches authenticated caller
    Note over N: ✓ @backup has consume permission
    Note over N: ✓ Service user → skip guardrails

    N-->>B: 200 {type:"aws", fields:{…}}
    B->>B: Use credential for backup upload
```

## Full Consumption Sequence

This shows what happens when a Go service calls `creds.GetCredential(id)` — the primary consumption path.

```mermaid
sequenceDiagram
    participant S as Go Service
    participant C as cbauth (CredsImpl)
    participant I as cbauthimpl.GetCredential
    participant N as ns_server<br/>/_cbauth/getCredential/:id

    S->>C: creds.GetCredential("my-aws-key")
    C->>I: GetCredential(svc, id, user, domain, extras)

    Note over I: Build URL:<br/>getCredentialBaseURL + "/" + id
    Note over I: Authenticate as specialUser<br/>(Basic Auth)
    Note over I: Pass on-behalf-of identity<br/>as query params:<br/>?user=...&domain=...&extras=...

    I->>N: GET /_cbauth/getCredential/my-aws-key<br/>Authorization: Basic @cbq-engine:pwd<br/>?user=alice&domain=local

    Note over N: 1. Identify calling service<br/>   from authenticated identity
    Note over N: 2. Validate caller identity
    Note over N: 3. Check consume RBAC
    Note over N: 4. Consume credential<br/>   (checks expiry)
    Note over N: 5. Service identity?<br/>   → bypass guardrails<br/>   End user?<br/>   → enforce allowedServices

    alt Success
        N-->>I: 200 + JSON {id, type,<br/>schemaVersion, meta, fields}
        I-->>C: *Credential (typed payload)
        C-->>S: *Credential, nil
    else Permission denied
        N-->>I: 403 INSUFFICIENT_PERMISSIONS
        I-->>C: ErrInsufficientPermissions
        C-->>S: nil, error
    else Service guardrail blocked
        N-->>I: 403 SERVICE_GUARDRAIL_BLOCKED
        I-->>C: ErrServiceGuardrailBlocked
        C-->>S: nil, error
    else Credential expired
        N-->>I: 403 CREDENTIAL_EXPIRED
        I-->>C: ErrStoredCredentialExpired
        C-->>S: nil, error
    else Not found
        N-->>I: 404
        I-->>C: ErrCredentialNotFound
        C-->>S: nil, error
    end
```

## Integration Checklist

If you are implementing credential consumption in a new service:

1. **Ensure prerequisites are met** — the credential store requires all nodes at Totoro (8.1)+, Enterprise edition, config encryption enabled, **and** n2n encryption enabled on all nodes before any credential operation succeeds, including consume.
   Without these, ns_server rejects the request.
   See [architecture.md — Storage](architecture.md#storage) for details and how to set overrides for the encryption requirements during development.

2. **Verify your service mapping exists** — your service must be listed in `misc:service_definitions/0` (`misc.erl`).
   Each entry maps a service atom to its `specialUser` identity name and optional `auth_aliases`.
   If your service is not listed, add it so that `identity_name_to_service/1` and `service_name_to_identity/1` resolve correctly.

3. **Choose your consumption pattern:**

   - **On-behalf-of an end user** (e.g. `@cbq-engine` for N1QL): The service authenticates end users via `cbauth.AuthWebCreds(req)` and passes their `Creds` object.
     The end user must hold `credential_consumer` for the credential, and the credential's `allowedServices` guardrail must list the service.

   - **As a service identity** (e.g. `@cbcontbk` for backup): The service uses its own `specialUser`/`specialPassword` (retrieved through `cbauth.GetHTTPServiceAuth`) to consume credentials directly.
     The service identity must hold `credential_consumer`.
     Service guardrails are bypassed.

4. **Call `creds.GetCredential(id)`** — cbauth handles:
   - Authenticating to ns_server as `specialUser` (your service).
   - Passing the user identity in query params.
   - Decoding the typed `Credential` struct.

5. **Handle errors** — check for the sentinel errors in the [error codes table](rest-api-reference.md#error-codes).
   Each maps to a specific user-facing condition.

6. **Enforce service-side guardrails** — after receiving a `Credential`, inspect `Meta.Guardrails` and enforce:
   - `URLWhitelist` (with `AllAccess`, `AllowedURLs`, `DisallowedURLs`) / `AllowedResources` / `AllowedOperations`
   - ns_server does NOT enforce these; your service must.

7. **Clear sensitive data** — zero out secret fields (passwords, keys) when no longer needed.

**Checking consume permission.** Services can verify whether a caller has permission to consume a credential using the `IsAllowed` method on the `Creds` object:

```go
allowed, err := creds.IsAllowed(
    "cluster.credentials[my-aws-key]!consume")
```

## Testing Credentials

### Create a credential

```bash
curl -X POST -u Administrator:password \
  -H "Content-Type: application/json" \
  -d '{
    "type": "aws",
    "fields": {
      "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
      "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "region": "us-east-1"
    },
    "description": "Test S3 credential",
    "guardrails": {
      "allowedServices": ["n1ql"]
    }
  }' \
  http://localhost:8091/settings/credentials/test-aws-key
```

### Grant consume to a service (e.g. backup)

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/services/backup/roles \
  -d "roles=credential_consumer[test-aws-key]"
```

### Grant consume to an end user

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/users/local/testuser \
  -d "roles=credential_consumer[test-aws-key]"
```

### Verify consume (end-to-end via cbauth)

This happens automatically in the service code:

```go
creds, err := cbauth.Default.AuthWebCreds(req) // or Auth(user, pwd)
if err != nil { /* handle */ }

credential, err := creds.GetCredential("test-aws-key")
switch {
case errors.Is(err, cbauth.ErrCredentialNotFound):
    // credential does not exist
case errors.Is(err, cbauth.ErrInsufficientPermissions):
    // user lacks consume permission
case errors.Is(err, cbauth.ErrServiceGuardrailBlocked):
    // service not in allowedServices
case errors.Is(err, cbauth.ErrStoredCredentialExpired):
    // credential has expired
case err != nil:
    // other error
default:
    // credential.AWS.AccessKeyID, credential.AWS.SecretAccessKey, etc.
}
```
