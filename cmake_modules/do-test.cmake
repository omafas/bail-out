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

SET(TEST_COMMAND "${ERL_EXECUTABLE}"
        -pa ${ebindirs}
        -pa "${COUCHDB_BIN_DIR}/couch/ebin"
        -pa "${COUCHDB_BIN_DIR}/mochiweb/ebin"
        -pa "${COUCHDB_BIN_DIR}/ejson/ebin"
        -pa "${COUCHDB_BIN_DIR}/couch_index_merger/ebin"
        -env OVERRIDE_EXECUTABLE_PATHS ${OVERRIDE_EXECUTABLE_PATHS}
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
