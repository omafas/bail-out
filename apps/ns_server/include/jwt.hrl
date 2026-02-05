%% @author Couchbase <info@couchbase.com>
%% @copyright 2025-Present Couchbase, Inc.
%%
%% Use of this software is governed by the Business Source License included in
%% the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
%% file, in accordance with the Business Source License, use of this software
%% will be governed by the Apache License, Version 2.0, included in the file
%% licenses/APL2.txt.

-define(JWT_SIGNING_KEYS_KEY, jwt_signing_keys).
-define(OIDC_PREAUTH_STORE_TTL_SECONDS, 60).
-define(OIDC_PREAUTH_STORE_SWEEP_INTERVAL_MS, 60000).

-define(JWT_ALGORITHMS,
        ['RS256', 'RS384', 'RS512', 'PS256', 'PS384', 'PS512',
         'ES256', 'ES256K', 'ES384', 'ES512',
         'EdDSA',
         'HS256', 'HS384', 'HS512']).

-type jwt_algorithm() ::
        'RS256' | 'RS384' | 'RS512' | 'PS256' | 'PS384' | 'PS512' |
        'ES256' | 'ES256K' | 'ES384' | 'ES512' |
        'EdDSA' |
        'HS256' | 'HS384' | 'HS512'.

-type jwt_kid_to_jwk() :: #{binary() | undefined => jose_jwk:key()}.
