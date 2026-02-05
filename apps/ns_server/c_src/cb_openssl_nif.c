/* @author Couchbase <info@couchbase.com>
 * @copyright 2025-Present Couchbase, Inc.
 *
 * Use of this software is governed by the Business Source License included in
 * the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
 * file, in accordance with the Business Source License, use of this software
 * will be governed by the Apache License, Version 2.0, included in the file
 * licenses/APL2.txt.
 */

#include <erl_nif.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/err.h>
#include <openssl/core_names.h>
#include <string.h>

static ERL_NIF_TERM kbkdf_hmac_nif(ErlNifEnv* env, int argc,
                                   const ERL_NIF_TERM argv[])
{
    if (argc != 5) {
        return enif_make_badarg(env);
    }

    ErlNifBinary key_bin, info_bin, salt_bin;
    char hmac_atom[16];
    char *hmac = NULL;
    const char *hmac_src = NULL;
    unsigned int out_len;

    /* Extract arguments */
    // This is erlang crypto:hmac_hash_algorithm()
    if (!enif_get_atom(env, argv[0], hmac_atom, 16, ERL_NIF_LATIN1)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[1], &key_bin)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &info_bin)) {
        return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[3], &salt_bin)) {
        return enif_make_badarg(env);
    }
    if (!enif_get_uint(env, argv[4], &out_len)) {
        return enif_make_badarg(env);
    }

    // Artificially limiting the output length, key length, and salt+info length
    // to make sure the generation is quick enough to be run in a regular erlang
    // scheduler. If we need to operate on larger data, we should consider
    // using dirty schedulers.
    // The limits are arbitrary and can be adjusted if needed.
    // Currently when all these limits are reached (worst case), the generation
    // takes about 4 ms which seems to be close to the max time for a nif
    // running in a regular erlang scheduler.
    // For expected usage (32 byte keys and 1024 byte salt+info), the generation
    // takes less than 100 microseconds.
    if (out_len > 512) {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "out_len_too_long"));
    }

    if (key_bin.size > 512) {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "in_key_len_too_long"));
    }

    if (info_bin.size + salt_bin.size > 524288) {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "input_data_too_long"));
    }

    if (strcmp(hmac_atom, "sha224") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA2_224;
    } else if (strcmp(hmac_atom, "sha256") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA2_256;
    } else if (strcmp(hmac_atom, "sha384") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA2_384;
    } else if (strcmp(hmac_atom, "sha512") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA2_512;
    } else if (strcmp(hmac_atom, "sha512_224") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA2_512_224;
    } else if (strcmp(hmac_atom, "sha512_256") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA2_512_256;
    } else if (strcmp(hmac_atom, "sha3_224") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA3_224;
    } else if (strcmp(hmac_atom, "sha3_256") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA3_256;
    } else if (strcmp(hmac_atom, "sha3_384") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA3_384;
    } else if (strcmp(hmac_atom, "sha3_512") == 0) {
        hmac_src = OSSL_DIGEST_NAME_SHA3_512;
    } else {
        return enif_make_badarg(env);
    }

    EVP_KDF_CTX* kctx = NULL;
    EVP_KDF* kdf = NULL;
    unsigned char* out = NULL;

    kdf = EVP_KDF_fetch(NULL, "KBKDF", NULL);
    if (kdf == NULL) {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "kdf_fetch_failed"));
    }

    kctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (kctx == NULL) {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "kdf_ctx_new_failed"));
    }

    /* Copy string literal to mutable buffer */
    size_t hmac_len = strlen(hmac_src) + 1;
    hmac = enif_alloc(hmac_len);
    if (hmac == NULL) {
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "hmac_alloc_failed"));
    }
    memcpy(hmac, hmac_src, hmac_len);

    OSSL_PARAM params[7];
    int p = 0;
    params[p++] = OSSL_PARAM_construct_utf8_string(
        OSSL_KDF_PARAM_MODE, "counter", 0);
    params[p++] = OSSL_PARAM_construct_utf8_string(
        OSSL_KDF_PARAM_MAC, "HMAC", 0);
    params[p++] = OSSL_PARAM_construct_utf8_string(
        OSSL_KDF_PARAM_DIGEST, hmac, strlen(hmac));
    params[p++] = OSSL_PARAM_construct_octet_string(
        OSSL_KDF_PARAM_KEY, (void*)key_bin.data, key_bin.size);
    if (info_bin.size > 0) {
        params[p++] = OSSL_PARAM_construct_octet_string(
            OSSL_KDF_PARAM_INFO, (void*)info_bin.data, info_bin.size);
    }
    if (salt_bin.size > 0) {
        params[p++] = OSSL_PARAM_construct_octet_string(
            OSSL_KDF_PARAM_SALT, (void*)salt_bin.data, salt_bin.size);
    }
    params[p] = OSSL_PARAM_construct_end();

    out = enif_alloc(out_len);
    if (out == NULL) {
        enif_free(hmac);
        EVP_KDF_CTX_free(kctx);
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "alloc_failed"));
    }

    if (EVP_KDF_derive(kctx, out, out_len, params) <= 0) {
        enif_free(hmac);
        enif_free(out);
        EVP_KDF_CTX_free(kctx);
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "kdf_derive_failed"));
    }

    /* Create Erlang binary with the result */
    ERL_NIF_TERM out_bin;
    unsigned char* out_ptr = enif_make_new_binary(env, out_len, &out_bin);
    if (out_ptr == NULL) {
        enif_free(hmac);
        enif_free(out);
        EVP_KDF_CTX_free(kctx);
        return enif_make_tuple2(env,
                                enif_make_atom(env, "error"),
                                enif_make_atom(env, "bin_alloc_failed"));
    }
    memcpy(out_ptr, out, out_len);

    enif_free(hmac);
    enif_free(out);
    EVP_KDF_CTX_free(kctx);
    return enif_make_tuple2(env, enif_make_atom(env, "ok"), out_bin);
}

static ErlNifFunc nif_funcs[] = {
    {"kbkdf_hmac", 5, kbkdf_hmac_nif}
};

ERL_NIF_INIT(cb_openssl, nif_funcs, NULL, NULL, NULL, NULL)
