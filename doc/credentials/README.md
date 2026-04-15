# Credential Store Documentation

The credential store lets administrators create named credentials (AWS keys, Azure tokens, GCP service accounts, HTTP auth, Couchbase remote-cluster creds, etc.) via REST.
Services consume these credentials at runtime through cbauth.

Two consumption patterns exist:

1. **On-behalf-of an end user** — the service authenticates as itself (e.g. `@cbq-engine`) and passes the end user's identity.
   The end user must hold the RBAC `consume` permission, **and** the service must be listed in the credential's `allowedServices` guardrail.

2. **As a service identity** — the service authenticates and consumes as itself (e.g. `@backup`).
   The service identity must hold the RBAC `consume` permission.
   Service guardrails are **bypassed**.

## Documents

| Document | Audience | Description |
|---|---|---|
| [REST API Reference](rest-api-reference.md) | UI / frontend developers | Endpoints, request/response formats, error codes, wire format, guardrails |
| [Service Integration Guide](service-integration-guide.md) | Go service developers | cbauth consumption patterns, sequence diagrams, integration checklist, testing |
| [Architecture & Design](architecture.md) | ns_server developers | Chronicle storage, encryption design, RBAC internals, authorization flowchart, known issues |
| [Credential Types](credential-types.md) | All | V1 schema envelope, meta fields, per-type field tables |

## Viewing Diagrams

Several documents use [Mermaid](https://mermaid.js.org/) for architecture
diagrams and sequence flows.
See [IDE setup instructions](../README.md#viewing-diagrams) for how to enable
Mermaid rendering in VS Code and IntelliJ.

