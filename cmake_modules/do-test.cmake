# Find all ebin directories and run the test.

IF (NOT DEFINED TEST_TARGET)
  SET (TEST_TARGET "$ENV{TEST_TARGET}")
  IF ("${TEST_TARGET}" STREQUAL "")
    SET (TEST_TARGET start)
  ENDIF ("${TEST_TARGET}" STREQUAL "")
ENDIF (NOT DEFINED TEST_TARGET)

FILE (GLOB ebindirs RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        # We must include:
        # 1) Any test profile beam files (needs to cover both ebin and test,
        #    depending on the configuration we are likely to have files in both
        #    locations
        _build/test/lib/*/ebin
        _build/test/lib/*/test
     )

STRING (RANDOM LENGTH 16 NODE_NAME_RANDOM)
SET (NODE_NAME "test-${NODE_NAME_RANDOM}")

# Get the paths for the the other executables we might spawn. This cmake file
# is executed outside of the context of the original cmake setup, so it does not
# have access to the CCache by default. We can read the individual variables
# that we need via LOAD_CACHE.
# First we need the list of binaries
LOAD_CACHE(${CCACHE_DIR} READ_WITH_PREFIX "MAIN_CACHE_" NS_TEST_BINARY_DEPS)

FOREACH (DEP ${MAIN_CACHE_NS_TEST_BINARY_DEPS})
  SET(cached_dep_build_dir "${DEP}_BINARY_DIR")
  # And now we can load their paths
  LOAD_CACHE(${CCACHE_DIR} READ_WITH_PREFIX "MAIN_CACHE_"
             "${cached_dep_build_dir}")

  SET(dep_build_dir "${MAIN_CACHE_${cached_dep_build_dir}}")
  SET(OVERRIDE_EXECUTABLE_PATHS
          "${DEP}=${dep_build_dir}:${OVERRIDE_EXECUTABLE_PATHS}")
ENDFOREACH(DEP ${MAIN_CACHE_NS_TEST_BINARY_DEPS})

# Load OpenSSL library path from cache so we can add it to LD_LIBRARY_PATH
# This allows Go binaries (like gosecrets) to find OpenSSL libraries at runtime
LOAD_CACHE(${CCACHE_DIR} READ_WITH_PREFIX "MAIN_CACHE_" OPENSSL_CRYPTO_LIBRARY)
SET(_openssl_crypto_lib "${MAIN_CACHE_OPENSSL_CRYPTO_LIBRARY}")

# Determine which library path environment variable to use based on platform
IF(APPLE)
  SET(_library_path_env_var "DYLD_LIBRARY_PATH")
ELSE()
  SET(_library_path_env_var "LD_LIBRARY_PATH")
ENDIF()

# Store library path in CMake variable so it persists for later use
SET(_library_path)
IF(_openssl_crypto_lib)
  # Extract directory from OPENSSL_CRYPTO_LIBRARY
  GET_FILENAME_COMPONENT(_openssl_lib_dir "${_openssl_crypto_lib}" DIRECTORY)
  GET_FILENAME_COMPONENT(_openssl_lib_dir "${_openssl_lib_dir}" ABSOLUTE)

  IF(EXISTS "${_openssl_lib_dir}")
    SET(_existing_path "$ENV{${_library_path_env_var}}")
    IF(_existing_path)
      SET(_library_path "${_openssl_lib_dir}:${_existing_path}")
      MESSAGE(STATUS "--- Added ${_openssl_lib_dir} to ${_library_path_env_var} (prepended to existing: ${_existing_path})")
    ELSE()
      SET(_library_path "${_openssl_lib_dir}")
      MESSAGE(STATUS "--- Set ${_library_path_env_var} to ${_library_path}")
    ENDIF()
  ELSE()
    MESSAGE(WARNING "--- OpenSSL library directory not found at ${_openssl_lib_dir}, skipping ${_library_path_env_var} setup")
  ENDIF()
ELSE()
  MESSAGE(WARNING "--- OPENSSL_CRYPTO_LIBRARY not found in cache, skipping ${_library_path_env_var} setup")
ENDIF()

# Build environment variable arguments for Erlang's -env flag
# This ensures LD_LIBRARY_PATH (Linux) or DYLD_LIBRARY_PATH (macOS) are passed to
# child processes spawned by Erlang (like gosecrets)
SET(_env_args)
IF(_library_path)
  LIST(APPEND _env_args "-env" "${_library_path_env_var}" "${_library_path}")
  MESSAGE(STATUS "--- Adding ${_library_path_env_var}=${_library_path} to Erlang -env")
ENDIF()

SET(TEST_COMMAND "${ERL_EXECUTABLE}"
        -pa ${ebindirs}
        -pa "${COUCHDB_BIN_DIR}/couch/ebin"
        -pa "${COUCHDB_BIN_DIR}/mochiweb/ebin"
        -pa "${COUCHDB_BIN_DIR}/ejson/ebin"
        -pa "${COUCHDB_BIN_DIR}/couch_index_merger/ebin"
        -env OVERRIDE_EXECUTABLE_PATHS ${OVERRIDE_EXECUTABLE_PATHS}
        ${_env_args}
        -noshell
        -kernel logger "[{handler, default, undefined}]"
        -shutdown_time 10000
        -sname "${NODE_NAME}"
        -eval "application:start(sasl)."
        # Need to escape ';' when we pass it to erl or it is treated
        # as two strings and that doesn't compile.
        -eval "
            case t:${TEST_TARGET}(${TEST_FILTER}) of
                ok -> init:stop()\;
                _ -> init:stop(1)
            end.")

EXECUTE_PROCESS(COMMAND "${CMAKE_COMMAND}" -E echo ${TEST_COMMAND})
EXECUTE_PROCESS(RESULT_VARIABLE _failure COMMAND ${TEST_COMMAND})
IF (_failure)
  MESSAGE (FATAL_ERROR "failed running tests")
ENDIF (_failure)
