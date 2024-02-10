# Generate .plt file, if it doesn't exist
GET_FILENAME_COMPONENT (_couchdb_bin_dir "${COUCHDB_BIN_DIR}" REALPATH)

IF (NOT EXISTS "${COUCHBASE_PLT}")
  MESSAGE ("Generating ${COUCHBASE_PLT}...")
  EXECUTE_PROCESS (
    COMMAND_ECHO STDOUT
    COMMAND "${DIALYZER_EXECUTABLE}" --output_plt "${COUCHBASE_PLT}" --build_plt
    --apps compiler crypto erts inets kernel os_mon sasl ssl stdlib xmerl eldap
           public_key
    ${_couchdb_bin_dir}/src/couchdb
    ${_couchdb_bin_dir}/src/couch_set_view
    ${_couchdb_bin_dir}/src/couch_view_parser
    ${_couchdb_bin_dir}/src/couch_index_merger
    ${_couchdb_bin_dir}/src/mapreduce
    ${_couchdb_bin_dir}/src/mochiweb
    ${_couchdb_bin_dir}/src/snappy
    ${_couchdb_bin_dir}/src/etap
    ${_couchdb_bin_dir}/src/lhttpc
    ${_couchdb_bin_dir}/src/erlang-oauth
    ${_couchdb_bin_dir}/src/ejson
    deps/gen_smtp
    deps/chronicle
    deps/enacl
    deps/esaml
    deps/iso8601)
ENDIF (NOT EXISTS "${COUCHBASE_PLT}")

# Compute list of .beam files
FILE (GLOB beamfiles RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" ebin/*.beam)
STRING (REGEX REPLACE "ebin/(couch_api_wrap(_httpc)?).beam\;?" "" beamfiles "${beamfiles}")

FILE (GLOB couchdb_beamfiles RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" deps/ns_couchdb/ebin/*.beam)
STRING (REGEX REPLACE "deps/ns_couchdb/ebin/couch_log.beam\;?" "" couchdb_beamfiles "${couchdb_beamfiles}")

EXECUTE_PROCESS (RESULT_VARIABLE _failure
  COMMAND_ECHO STDOUT
  COMMAND "${DIALYZER_EXECUTABLE}" --plt "${COUCHBASE_PLT}" ${DIALYZER_FLAGS}
  --apps ${beamfiles}
  deps/ale/ebin
  deps/ns_babysitter/ebin
  ${couchdb_beamfiles})
IF (_failure)
  MESSAGE (FATAL_ERROR "failed running dialyzer")
ENDIF (_failure)
