# Credential Types — V1 Schema Reference

This document describes the credential type schemas supported by the credential store.
For the overall architecture see [architecture.md](architecture.md).
For the REST API see [rest-api-reference.md](rest-api-reference.md).

All credential types share the same envelope:

```json
{
  "id": "<string>",
  "type": "<credential_type>",
  "schemaVersion": 1,
  "meta": { ... },
  "fields": { ... }
}
```

## Meta (common to all types)

| Field | Type | Required | Description |
|---|---|---|---|
| `description` | string | No | Human-readable description |
| `createdAt` | integer (ms) | Yes | Creation timestamp (ms since Unix epoch) |
| `createdBy` | `{user, domain}` | Yes | Author who created the credential |
| `updatedAt` | integer (ms) | No | Last-update timestamp |
| `updatedBy` | `{user, domain}` | No | Author who last updated |
| `expiresAt` | integer (ms) | No | Expiry timestamp; must be >= 5 min in the future at creation |
| `guardrails` | object | No | Usage restrictions (see [Guardrails](rest-api-reference.md#guardrails)) |
| `payloadVersion` | string | Yes | Opaque revision for optimistic concurrency |

## `aws`

Go struct: `AWSPayload`

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `accessKeyId` | `access_key_id` | string | **Yes** | No | AWS access key ID |
| `secretAccessKey` | `secret_access_key` | string | **Yes** | **Yes** | AWS secret access key |
| `region` | `region` | string | **Yes** | No | AWS region (e.g. `us-east-1`) |
| `endpoint` | `endpoint` | string | No | No | S3-compatible endpoint override |
| `sessionToken` | `session_token` | string | No | **Yes** | Temporary session token (STS) |

## `azureShared`

Go struct: `AzureSharedPayload`

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `accountName` | `account_name` | string | **Yes** | No | Azure storage account name |
| `accountKey` | `account_key` | string | **Yes** | **Yes** | Azure storage account key |
| `endpoint` | `endpoint` | string | No | No | Custom endpoint override |

## `azureAd`

Go struct: `AzureADPayload`

Exactly one of `clientSecret` or `certificate` must be provided.

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `clientId` | `client_id` | string | **Yes** | No | Azure AD application client ID |
| `tenantId` | `tenant_id` | string | **Yes** | No | Azure AD tenant ID |
| `clientSecret` | `client_secret` | string | No\* | **Yes** | Client secret (mutual exclusive with cert) |
| `certificate` | `certificate` | cert_pem | No\* | No | Client certificate PEM |
| `certPassword` | `cert_password` | string | No | **Yes** | Password for encrypted cert |
| `endpoint` | `endpoint` | string | No | No | Custom endpoint override |

\* Exactly one of `clientSecret` or `certificate` is required.

## `azureSas`

Go struct: `AzureSASPayload`

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `accountName` | `account_name` | string | **Yes** | No | Azure storage account name |
| `sharedAccessSignature` | `shared_access_signature` | string | **Yes** | **Yes** | SAS token |
| `endpoint` | `endpoint` | string | No | No | Custom endpoint override |

## `azureManaged`

Go struct: `AzureManagedPayload`

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `managedIdentityId` | `managed_identity_id` | string | No | No | Managed identity client ID (optional for system-assigned) |
| `endpoint` | `endpoint` | string | No | No | Custom endpoint override |

## `gcp`

Go struct: `GCPPayload`

Two mutually exclusive modes:
- **Service-account mode**: `jsonCredentials` is set.
- **HMAC mode**: `accessKeyId` + `secretAccessKey` are set.

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `jsonCredentials` | `json_credentials` | json_object | No\* | **Yes** | GCP service-account JSON key file |
| `accessKeyId` | `access_key_id` | string | No\* | No | HMAC access key |
| `secretAccessKey` | `secret_access_key` | string | No\* | **Yes** | HMAC secret key |
| `region` | `region` | string | No | No | GCS region |
| `endpoint` | `endpoint` | string | No | No | Custom endpoint override |

\* Either `jsonCredentials` or both `accessKeyId` + `secretAccessKey` are required.

## `http`

Go struct: `HTTPPayload`

Scheme-specific required fields:
- `basic`: requires `username` + `password`
- `bearer`: requires `token`
- `mtls`: requires `certificate` + `privateKey`

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `authScheme` | `auth_scheme` | enum | **Yes** | No | `"basic"`, `"bearer"`, or `"mtls"` |
| `username` | `username` | string | Scheme\* | No | Username (basic auth) |
| `password` | `password` | string | Scheme\* | **Yes** | Password (basic auth) |
| `headerName` | `header_name` | string | No | No | Custom header name (bearer) |
| `token` | `token` | string | Scheme\* | **Yes** | Bearer token |
| `certificate` | `certificate` | cert_pem | Scheme\* | No | Client certificate PEM (mTLS) |
| `privateKey` | `private_key` | pkey_pem | Scheme\* | **Yes** | Client private key PEM (mTLS) |
| `passphrase` | `passphrase` | string | No | **Yes** | Passphrase for encrypted private key |
| `rootCertificate` | `root_certificate` | cert_pem | No | No | CA / root certificate PEM |
| `skipVerify` | `skip_verify` | boolean | No | No | Skip TLS verification (dev only) |

## `couchbase`

Go struct: `CouchbasePayload`

| JSON Field | Storage Key | Type | Required | Sensitive | Description |
|---|---|---|---|---|---|
| `encryptionType` | `encryption_type` | enum | **Yes** | No | `"none"`, `"half"`, or `"full"` |
| `username` | `username` | string | No | No | Remote cluster username |
| `password` | `password` | string | No | **Yes** | Remote cluster password |
| `certificate` | `certificate` | cert_pem | No | No | Client certificate PEM |
| `privateKey` | `private_key` | pkey_pem | No | **Yes** | Client private key PEM |
| `passphrase` | `passphrase` | string | No | **Yes** | Passphrase for encrypted private key |
| `rootCertificate` | `root_certificate` | cert_pem | No | No | Remote cluster CA cert PEM |
