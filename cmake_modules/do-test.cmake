# Find all ebin directories and run the test.

IF (NOT DEFINED TEST_TARGET)
  SET (TEST_TARGET "$ENV{TEST_TARGET}")
  IF ("${TEST_TARGET}" STREQUAL "")
    SET (TEST_TARGET start)
  ENDIF ("${TEST_TARGET}" STREQUAL "")
ENDIF (NOT DEFINED TEST_TARGET)

FILE (GLOB ebindirs RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
  ebin deps/*/ebin deps/*/deps/*/ebin)
# Bug in CMake?
STRING (REGEX REPLACE "//" "/" ebindirs "${ebindirs}")

FILE (GLOB eunitdirs
  RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
  .eunit deps/*/.eunit)
# Bug in CMake?
STRING (REGEX REPLACE "//" "/" eunitdirs "${eunitdirs}")

STRING (RANDOM LENGTH 16 NODE_NAME_RANDOM)
SET (NODE_NAME "test-${NODE_NAME_RANDOM}")

# If you update the test command, please also update this echo command
# (including the silly escaped quotes) so it displays what is
# invoked. Yes, this is annoying.
EXECUTE_PROCESS(COMMAND "${CMAKE_COMMAND}" -E echo
  "${ERL_EXECUTABLE}"
  -pa ${ebindirs} ${eunitdirs}
  -pa "${COUCHDB_BIN_DIR}/src/couchdb"
  -pa "${COUCHDB_BIN_DIR}/src/mochiweb"
  -pa "${COUCHDB_BIN_DIR}/src/ejson"
  -pa "${COUCHDB_BIN_DIR}/src/couch_index_merger/ebin"
  -noshell -shutdown_time 10000
  -sname "${NODE_NAME}"
  -eval "\"application:start(sasl).\""
  -eval "\"case t:${TEST_TARGET}() of ok -> init:stop(); _ -> init:stop(1) end.\"")

EXECUTE_PROCESS(RESULT_VARIABLE _failure
  COMMAND "${ERL_EXECUTABLE}"
  # prefer eunitdirs
  -pa ${ebindirs} ${eunitdirs}
  -pa "${COUCHDB_BIN_DIR}/src/couchdb"
  -pa "${COUCHDB_BIN_DIR}/src/mochiweb"
  -pa "${COUCHDB_BIN_DIR}/src/ejson"
  -pa "${COUCHDB_BIN_DIR}/src/couch_index_merger/ebin"
  -noshell -shutdown_time 10000
  -sname "${NODE_NAME}"
  -eval "application:start(sasl)."
  -eval "case t:${TEST_TARGET}() of ok -> init:stop(); _ -> init:stop(1) end.")
IF (_failure)
  MESSAGE (FATAL_ERROR "failed running tests")
ENDIF (_failure)
