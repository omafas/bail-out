# Credential Store — REST API Reference

This document covers the REST endpoints, request/response formats, error codes, and wire format for the credential store.
For the overall architecture see [architecture.md](architecture.md).
For credential type schemas see [credential-types.md](credential-types.md).

## Admin CRUD Endpoints — `/settings/credentials`

These are the endpoints an administrator uses to create, read, update, and delete credentials.

| Method | Endpoint | Roles That Satisfy | Description |
|---|---|---|---|
| GET | `/settings/credentials` | Security Admin, RO Security Admin, Full Admin | List all credentials (+ warnings, optional `?prefix=`) |
| GET | `/settings/credentials/:id` | Security Admin, RO Security Admin, Full Admin | Get one credential (secrets redacted) |
| POST | `/settings/credentials/:id` | Security Admin, Full Admin | Create a credential |
| PUT | `/settings/credentials/:id` | Security Admin, Full Admin | Full replace — used to rotate credential material; `type` is immutable. For metadata-only edits use PATCH. |
| PATCH | `/settings/credentials/:id` | Security Admin, Full Admin | Partial metadata update — accepts only `description`, `expiresAt`, `guardrails`. Omitted keys are preserved; explicit JSON `null` clears the field. Never changes `type` or `fields`. Empty bodies are rejected. |
| DELETE | `/settings/credentials/:id` | Security Admin, Full Admin | Delete a credential (also removes all `credential_consumer[<id>]` grants referencing it from users and service roles) |

**Credential ID constraints:**

- A credential ID is a string up to **128 characters** long.
- Must contain only **ASCII printable characters** (no spaces).
- The id is an opaque string. `/` is allowed (e.g. `backup/prod/s3`) but has no special meaning; `prefix/*` matching in role grants is literal string-prefix matching.

## Credential Store Settings — `/settings/credentialStore`

| Method | Endpoint | Roles That Satisfy | Description |
|---|---|---|---|
| GET | `/settings/credentialStore` | Security Admin, RO Security Admin, Full Admin | Read store settings (+ warnings) |
| PUT | `/settings/credentialStore` | Security Admin, Full Admin | Update store settings |
| DELETE | `/settings/credentialStore` | Security Admin, Full Admin | Reset store settings to defaults |

**Settings fields (PUT body, JSON):**

| Field | Type | Default | Description |
|---|---|---|---|
| `configEncryptionOverride` | boolean | `false` | When `false`, enforces a strict link between credentials and encryption. Blocks disabling config encryption if credentials exist, and blocks credential CRUD operations if config encryption is disabled. Set to `true` to bypass these restrictions. |
| `n2nEncryptionOverride` | boolean | `false` | When `false` enforces a best-effort link between credentials and node-to-node encryption. Attempts to block disabling N2N encryption if credentials exist, and attempts to block credential CRUD operations if N2N encryption is disabled. Set to `true` to bypass these restrictions. |

Both fields are **required** in the PUT body.

**GET response** includes the current settings and, if credentials exist in the store, a `warnings` array when config encryption is disabled or n2n encryption is not fully enabled across the cluster.
Example:

```json
{
  "configEncryptionOverride": true,
  "n2nEncryptionOverride": false,
  "warnings": [
    "Stored credentials are not protected by config encryption at rest"
  ]
}
```

See [architecture.md — Storage](architecture.md#storage) for full details on the encryption requirements and the rationale for these overrides.

## Service Role Management — `/settings/rbac/services/:name/roles`

| Method | Endpoint | Roles That Satisfy | Description |
|---|---|---|---|
| GET | `/settings/rbac/services/:name/roles` | **Full Admin only** | Read current service roles |
| PUT | `/settings/rbac/services/:name/roles` | **Full Admin only** | Assign roles to a service |
| DELETE | `/settings/rbac/services/:name/roles` | **Full Admin only** | Delete all service roles |

> Security Admin **cannot** read, write, or delete service roles — this is by design to prevent privilege escalation.
> Service identities (`@`-prefixed callers in the admin domain) hold `service_admin`, which denies `cluster.admin.security!write`, so the RBAC layer rejects PUT and DELETE before the handler. Services may still read their own roles via GET.

## Granting Consume Permissions

### Granting consume permission to an end user

Use the standard user-management endpoint to assign the `credential_consumer` role with a credential ID pattern.

**Endpoint:** `PUT /settings/rbac/users/:domain/:userId`

**Callers:** **User Admin** (local or external) or **Full Admin**.
Since `credential_consumer` is not a security role, Security Admin is NOT required.

**Example — grant `alice` consume on credential `n1ql/prod/s3`:**

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/users/local/alice \
  -d "roles=credential_consumer[n1ql/prod/s3]"
```

**Example — grant `alice` consume on all credentials under `n1ql/`:**

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/users/local/alice \
  -d "roles=credential_consumer[n1ql/*]"
```

**Example — grant `alice` consume on ALL credentials:**

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/users/local/alice \
  -d "roles=credential_consumer[*]"
```

**Credential id pattern.** The bracket parameter accepts:

- `*` — any credential (wildcard).
- `prefix/*` — any credential whose id starts with `prefix/`. At least one such credential must exist.
- A concrete id — must exist in the credential store.

See [architecture.md — RBAC Model](architecture.md#rbac-model) for the full pattern semantics and grant lifecycle on credential deletion.

### Granting consume permission to a service identity

Services have a dedicated endpoint for managing their roles.
Only the `credential_consumer` role is permitted for services.

**Endpoint:** `PUT /settings/rbac/services/:serviceName/roles`

**Callers:** **Full Admin only**.
Security Admin and User Admin cannot use this endpoint — this ensures only the highest-privilege administrator can grant credentials to service identities.

**`:serviceName`** is one of: `n1ql`, `backup`, `index`, `xdcr`, `fts`, `eventing`, `cbas`

**Example — grant backup service consume on `backup/prod/s3`:**

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/services/backup/roles \
  -d "roles=credential_consumer[backup/prod/s3]"
```

**Example — grant backup service consume on ALL credentials:**

```bash
curl -X PUT -u Administrator:password \
  http://localhost:8091/settings/rbac/services/backup/roles \
  -d "roles=credential_consumer[*]"
```

**Read current service roles:**

```bash
curl -u Administrator:password \
  http://localhost:8091/settings/rbac/services/backup/roles
```

**Delete all service roles:**

```bash
curl -X DELETE -u Administrator:password \
  http://localhost:8091/settings/rbac/services/backup/roles
```

The same `credential_id` pattern rules apply as for end users: `*` for any, `prefix/*` for a matching prefix (at least one credential must match), or a concrete existing id. See [architecture.md — RBAC Model](architecture.md#rbac-model).

## Per-Credential Access Review

To answer "who can consume this credential?" or "which roles confer consume on this credential?", filter the standard RBAC listing endpoints by a `cluster.credentials[<id>]!consume` permission string.

**Endpoint:** `GET /settings/rbac/roles?permission=cluster.credentials/<id>!consume`
**Endpoint:** `GET /settings/rbac/users?permission=cluster.credentials/<id>!consume`

**Callers:** Full Admin, Security Admin, User Admin (read-only access to RBAC listings).

**What you get back depends on the id form:**

| Query | Rows returned for `credential_consumer` |
|---|---|
| `cluster.credentials[<id>]!consume` | One row for the concrete id and one for the `*` wildcard — both confer consume on `<id>` |
| `cluster.credentials[*]!consume` | Only the `*` wildcard row |
| `cluster.credentials[prefix/*]!consume` | The `*` wildcard row, plus any concrete-id rows whose ids start with `prefix/` |

**Example — list all roles that grant consume on `backup/prod/s3`:**

```bash
curl -u Administrator:password \
  'http://localhost:8091/settings/rbac/roles?permission=cluster.credentials%5Bbackup%2Fprod%2Fs3%5D%21consume'
```

**Example — list all users that can consume `backup/prod/s3`:**

```bash
curl -u Administrator:password \
  'http://localhost:8091/settings/rbac/users?permission=cluster.credentials%5Bbackup%2Fprod%2Fs3%5D%21consume'
```

The user listing includes anyone with `credential_consumer[backup/prod/s3]`, anyone with a matching prefix (e.g. `credential_consumer[backup/*]`), and anyone with the `*` wildcard.

## Guardrails

Guardrails are optional restrictions set when a credential is created or updated.
ns_server enforces **only** `allowedServices`; all other guardrails are the consuming service's responsibility.

| Guardrail | Enforced By | Description |
|---|---|---|
| `allowedServices` | ns_server | Which services may consume (end-user path) |
| `urlWhitelist` | Service | URL allow/disallow lists + allAccess |
| `allowedResources` | Service | Resource-level restrictions |
| `allowedOperations` | Service | Operation restrictions (READ, LIST …) |

### `allowedServices` valid values

`n1ql`, `backup`, `index`, `xdcr`, `fts`, `eventing`, `cbas`

These map to service identities via `misc:identity_name_to_service/1`:

| Service | Identity(ies) |
|---|---|
| n1ql | @cbq-engine, @n1ql, @query |
| backup | @backup, @cbcontbk |
| index | @index, @projector |
| xdcr | @goxdcr |
| fts | @fts |
| eventing | @eventing |
| cbas | @cbas |

### `urlWhitelist` sub-object

| JSON Field | Type | Description |
|---|---|---|
| `allAccess` | boolean | When `true`, permit all URLs |
| `allowedUrls` | string[] | URL patterns that are allowed |
| `disallowedUrls` | string[] | URL patterns that are explicitly blocked |

## Error Codes

Errors returned from `/_cbauth/getCredential/:id` and mapped to Go sentinel errors in `cbauthimpl`:

| HTTP | Error Code | Go Error | When |
|---|---|---|---|
| 403 | `INSUFFICIENT_PERMISSIONS` | `ErrInsufficientPermissions` | User lacks consume RBAC permission |
| 403 | `SERVICE_GUARDRAIL_BLOCKED` | `ErrServiceGuardrailBlocked` | Service not in allowedServices (end user) |
| 403 | `CREDENTIAL_EXPIRED` | `ErrStoredCredentialExpired` | Credential's expiresAt has passed |
| 404 | *(no body)* | `ErrCredentialNotFound` | Credential ID does not exist |
| 503 | `UNSUPPORTED_SCHEMA_VERSION` | `ErrSchemaVersionUnsupported` | Schema version not supported by this build |

## Wire Format (JSON response)

```json
{
  "id": "my-aws-key",
  "type": "aws",
  "schemaVersion": 1,
  "meta": {
    "description": "Production S3 access",
    "createdAt": 1740000000000,
    "createdBy": {"user": "Administrator", "domain": "admin"},
    "expiresAt": 1750000000000,
    "guardrails": {
      "allowedServices": ["n1ql", "index"],
      "urlWhitelist": {
        "allAccess": false,
        "allowedUrls": ["https://s3.amazonaws.com/*"],
        "disallowedUrls": []
      }
    },
    "payloadVersion": "g2gCZAAIY..."
  },
  "fields": {
    "accessKeyId": "AKIA...",
    "secretAccessKey": "wJalrXU...",
    "region": "us-east-1"
  }
}
```

See [credential-types.md](credential-types.md) for the full field reference for each credential type.
