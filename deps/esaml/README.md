An implementation of the Security Assertion Markup Language (SAML) in Erlang. So far this supports enough of the standard to act as a Service Provider (SP) to perform authentication with SAML. It has been tested extensively against the SimpleSAMLphp IdP and can be used in production.

Please read [this](CONTRIBUTORS.md) in order to make contributions.

### Supported protocols

The SAML standard refers to a flow of request/responses that make up one concrete action as a "protocol". Currently all of the basic Single-Sign-On and Single-Logout protocols are supported. There is no support at present for the optional Artifact Resolution, NameID Management, or NameID Mapping protocols.

Future work may add support for the Assertion Query protocol (which is useful to check if SSO is already available for a user without demanding they authenticate immediately).

Single sign-on protocols:

 * SP: send AuthnRequest (REDIRECT or POST) -> receive Response + Assertion (POST)

Single log-out protocols:

 * SP: send LogoutRequest (REDIRECT) -> receive LogoutResponse (REDIRECT or POST)
 * SP: receive LogoutRequest (REDIRECT OR POST) -> send LogoutResponse (REDIRECT)

`esaml` supports RSA+SHA1/SHA256 signing of all SP payloads, and validates signatures on all IdP responses. Compatibility flags are available to disable verification where IdP implementations lack support (see the [esaml_sp record](http://arekinath.github.io/esaml/esaml.html#type-sp), and members such as `idp_signs_logout_requests`).

### Assertion Encryption

The following algorithms are supported:

| Encryption | Algorithms |
|:---------- |:---------- |
| Key Encryption | `http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p`<br/>`http://www.w3.org/2001/04/xmlenc#rsa-1_5` |
| Data Encryption | `http://www.w3.org/2009/xmlenc11#aes128-gcm`<br/>`http://www.w3.org/2001/04/xmlenc#aes128-cbc`<br/>`http://www.w3.org/2001/04/xmlenc#aes256-cbc` |

### API documentation

Edoc documentation for the whole API is available at:

https://hexdocs.pm/esaml

### Licensing

2-clause BSD

### Getting started

The simplest way to use `esaml` in your app is with the `esaml_cowboy` module. There are two SAML Server Provider (SP) applications included in the repo under `examples` directory.

The application in `examples/sp` directory shows how you can use `esaml` to enabled Single-Sign-On (SSO) in your application. This application enables an endpoint that supports Server Provider metadata request, SAML authentication request as well as the ability to consume the response from IdP.

The second application in `example/sp_with_logout` shows how Single Logout can be enabled. It also shows how you can build a bridge from `esaml` to local application session storage, by generating session cookies for each user that logs in (and storing them in ETS).

### More advanced usage

You can also tap straight into lower-level APIs in `esaml` if `esaml_cowboy` doesn't meet your needs. The `esaml_binding` and `esaml_sp` modules are the interface used by `esaml_cowboy` itself, and contain all the basic primitives to generate and parse SAML payloads.

This is particularly useful if you want to implement SOAP endpoints using SAML.

> The Elixir library `Samly` is one such implementation. It dose not use `esaml_cowboy`. Instead it relies on the lower-level APIs and uses Elixir `Plug` and `Cowboy` directly for endpoints/routing.
