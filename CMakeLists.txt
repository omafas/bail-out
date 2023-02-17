CMAKE_MINIMUM_REQUIRED (VERSION 3.15)

PROJECT (ns_server)
INCLUDE (FindCouchbaseErlang)
INCLUDE (FindCouchbaseGo)
INCLUDE (CouchbasePlugInUI)
INCLUDE (ParseArguments)
INCLUDE (CTest)

# Installation subdir
SET (INST_LIBDIR "lib/ns_server/erlang/lib")

# If PRODUCT_VERSION is set externally, derive product version string based on
# it and BUILD_ENTERPRISE. Otherwise, fallback to output of 'git describe'.

IF ("${PRODUCT_VERSION}" STREQUAL "")
  # Cache "git describe" output
  EXECUTE_PROCESS(COMMAND git describe
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE _product_version
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE)
ELSE ("${PRODUCT_VERSION}" STREQUAL "")
  SET(_product_version "${PRODUCT_VERSION}")
ENDIF ("${PRODUCT_VERSION}" STREQUAL "")

IF (BUILD_ENTERPRISE)
  SET (_ns_version "${_product_version}-enterprise")
ELSE (BUILD_ENTERPRISE)
  SET (_ns_version "${_product_version}-community")
ENDIF (BUILD_ENTERPRISE)

SET (NS_SERVER_VERSION "${_ns_version}" CACHE STRING
     "Couchbase server version" FORCE)
MARK_AS_ADVANCED (NS_SERVER_VERSION)

# Create rebar.config
SET (COUCHDB_SRC_DIR "${PROJECT_SOURCE_DIR}/../couchdb" CACHE PATH "Path to couchdb source")
SET (COUCHDB_BIN_DIR "${CMAKE_BINARY_DIR}/couchdb" CACHE PATH "Path to couchdb build directory")

IF (NOT EXISTS "${COUCHDB_SRC_DIR}/src/couchdb/couch_db.hrl")
  MESSAGE (FATAL_ERROR "Could not find couch_db.hrl; please re-run CMake with "
    "-DCOUCHDB_SRC_DIR=<path/to/couchdb source>")
ENDIF (NOT EXISTS "${COUCHDB_SRC_DIR}/src/couchdb/couch_db.hrl")

FILE (WRITE "${PROJECT_BINARY_DIR}/cluster_run.configuration"
  "prefix=\"${CMAKE_INSTALL_PREFIX}\"\n")

GET_FILENAME_COMPONENT (_couchdb_src "${COUCHDB_SRC_DIR}" REALPATH)
GET_FILENAME_COMPONENT (_couchdb_bin "${COUCHDB_BIN_DIR}" REALPATH)
FILE (WRITE "${PROJECT_BINARY_DIR}/config.mk"
  "COUCHDB_SRC_DIR = ${_couchdb_src}\n"
  "COUCHDB_BIN_DIR = ${_couchdb_bin}\n")

#build UI
SET (NS_UI_SRC "${PROJECT_SOURCE_DIR}/priv/public/ui")
SET (NS_UI_STAMP "${NS_UI_BUILD_DIR}/ns")
SET (NS_UI_BIN "${NS_UI_PUB_DIR}/ui")

FILE (MAKE_DIRECTORY "${NS_UI_PUB_DIR}")

SET (_out_js_stamp_depends "${NS_UI_STAMP}.js.stamp")
FUNCTION (_maybe_add_pluggable_ui_stamp p_ui_dir p_ui_stamp)
  IF (EXISTS "${CMAKE_SOURCE_DIR}/${p_ui_dir}")
    SET (_out_js_stamp_depends ${_out_js_stamp_depends} "${NS_UI_BUILD_DIR}/${p_ui_stamp}" PARENT_SCOPE)
  ENDIF()
ENDFUNCTION (_maybe_add_pluggable_ui_stamp)

_maybe_add_pluggable_ui_stamp("cbft" "fts.js.stamp")
_maybe_add_pluggable_ui_stamp("query-ui" "query.js.stamp")

IF (BUILD_ENTERPRISE)
  _maybe_add_pluggable_ui_stamp("goproj/src/github.com/couchbase/eventing" "event.js.stamp")
  _maybe_add_pluggable_ui_stamp("cbbs" "backup.js.stamp")
  _maybe_add_pluggable_ui_stamp("cbas-ui" "cbas.js.stamp")
ENDIF()

SET(NS_UI_PUB_JS_OUT_DIR "${NS_UI_PUB_DIR}/jsout")
SET(NS_UI_PUB_CSS_OUT_DIR "${NS_UI_PUB_DIR}/cssout")
FILE (GLOB_RECURSE UI_SRC_CSS_FILES CONFIGURE_DEPENDS "${NS_UI_SRC}/*.css")

ADD_CUSTOM_COMMAND (OUTPUT "${NS_UI_BUILD_DIR}/out.js.stamp"
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${NS_UI_PUB_JS_OUT_DIR}"
  COMMAND "${PROJECT_BINARY_DIR}/deps/gomodules/minify_js/minify_js"
  "--in-dir=${NS_UI_PUB_DIR}"
  "--out-dir=${NS_UI_PUB_JS_OUT_DIR}"
  "--importmap-path=${NS_UI_SRC}/importmap.json"
  COMMAND "${CMAKE_COMMAND}" -E touch "${NS_UI_BUILD_DIR}/out.js.stamp"
  DEPENDS ns_minify_js
  DEPENDS "${_out_js_stamp_depends}"
  VERBATIM)
ADD_CUSTOM_COMMAND (OUTPUT "${NS_UI_BUILD_DIR}/out.css.stamp"
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${NS_UI_PUB_CSS_OUT_DIR}"
  COMMAND "${PROJECT_BINARY_DIR}/deps/gomodules/minify_css/minify_css"
  "--in-path=${NS_UI_PUB_DIR}/ui/index.css"
  "--out-dir=${NS_UI_PUB_CSS_OUT_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E touch "${NS_UI_BUILD_DIR}/out.css.stamp"
  DEPENDS ns_minify_css
  DEPENDS ${UI_SRC_CSS_FILES}
  VERBATIM)

ADD_CUSTOM_TARGET (ui_build ALL
  DEPENDS "${NS_UI_BUILD_DIR}/out.js.stamp" "${NS_UI_BUILD_DIR}/out.css.stamp"
  VERBATIM)
WATCH_AND_COPY_SOURCES (${NS_UI_SRC} ${NS_UI_BIN} ${NS_UI_STAMP} "ns")

FILE (GLOB NS_UI_ICONS_IMGS_ETC "${NS_UI_SRC}/../*.*")
FILE (COPY ${NS_UI_ICONS_IMGS_ETC} DESTINATION "${NS_UI_PUB_DIR}")

INSTALL (DIRECTORY "${PROJECT_SOURCE_DIR}/priv/public/"
  DESTINATION "${NS_UI_INSTALL_DIR}"
  PATTERN "*_license_agreement.txt" EXCLUDE
  PATTERN "*-dev.html" EXCLUDE
  PATTERN "node_modules/" EXCLUDE
  PATTERN "*.js" EXCLUDE)
INSTALL (DIRECTORY "${NS_UI_PUB_DIR}/jsout"
  DESTINATION "${NS_UI_INSTALL_DIR}")
INSTALL (DIRECTORY "${NS_UI_PUB_DIR}/cssout"
  DESTINATION "${NS_UI_INSTALL_DIR}")
INSTALL (FILES
  "${NS_UI_SRC}/libs/reflect-metadata.js"
  "${NS_UI_SRC}/libs/zone.js"
  DESTINATION "${NS_UI_INSTALL_DIR}/ui/libs")
INSTALL (FILES
  "${NS_UI_SRC}/libs/ace/worker-javascript.js"
  "${NS_UI_SRC}/libs/ace/worker-json.js"
  DESTINATION "${NS_UI_INSTALL_DIR}/ui/libs/ace")

# Various cleaning targets
ADD_CUSTOM_TARGET (ns_realclean
  COMMAND "${CMAKE_COMMAND}" -E remove -f cov.html erl_crash.dump
  COMMAND "${CMAKE_COMMAND}" -E remove_directory test/log
  COMMAND "${CMAKE_COMMAND}" -E remove -f rebar.config
  COMMAND "${CMAKE_COMMAND}" -E remove -f src/ns_server.app.src
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  VERBATIM)
ADD_DEPENDENCIES (realclean ns_realclean)

ADD_CUSTOM_TARGET (ns_dataclean
  COMMAND "${CMAKE_COMMAND}" -E remove_directory data
  COMMAND "${CMAKE_COMMAND}" -E remove_directory coverage
  COMMAND "${CMAKE_COMMAND}" -E remove_directory couch
  COMMAND "${CMAKE_COMMAND}" -E remove_directory logs
  COMMAND "${CMAKE_COMMAND}" -E remove_directory tmp
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  VERBATIM)

SET (ns_server_app_src "${PROJECT_SOURCE_DIR}/src/ns_server.app.src")
SET (rebar_config "${PROJECT_SOURCE_DIR}/rebar.config")
ADD_CUSTOM_TARGET (ns_cfg DEPENDS "${ns_server_app_src}" "${rebar_config}")
ADD_CUSTOM_COMMAND(OUTPUT "${ns_server_app_src}"
  COMMAND "${CMAKE_COMMAND}" -D "INPUT_FILE=${ns_server_app_src}.in"
  -D "OUTPUT_FILE=${ns_server_app_src}"
  -D "SEARCH=NS_SERVER_VERSION"
  -D "REPLACE=${NS_SERVER_VERSION}"
  -P "${PROJECT_SOURCE_DIR}/cmake_modules/string-replace.cmake"
  VERBATIM)
ADD_CUSTOM_COMMAND(OUTPUT "${rebar_config}"
  COMMAND "${CMAKE_COMMAND}" -D "INPUT_FILE=${rebar_config}.in"
  -D "OUTPUT_FILE=${rebar_config}"
  -D "SEARCH=COUCHDB_SRC_DIR"
  -D "REPLACE=${COUCHDB_SRC_DIR}"
  -D "NS_SERVER_SRC_DIR=${PROJECT_SOURCE_DIR}"
  -P "${PROJECT_SOURCE_DIR}/cmake_modules/string-replace.cmake"
  VERBATIM)

IF (NOT ESCRIPT_EXECUTABLE)
  MESSAGE (FATAL_ERROR "escript not found")
ENDIF (NOT ESCRIPT_EXECUTABLE)

SET (kv_stats_mapping_src "${PROJECT_SOURCE_DIR}/priv/kv_stats_mappings.txt")
SET (kv_stats_mapping_target "${PROJECT_SOURCE_DIR}/src/kv_stats_mappings.erl")
ADD_CUSTOM_TARGET (kv_mappings DEPENDS "${kv_stats_mapping_target}")
ADD_CUSTOM_COMMAND(OUTPUT "${kv_stats_mapping_target}"
  COMMAND "${ESCRIPT_EXECUTABLE}"
          "${PROJECT_SOURCE_DIR}/scripts/generate-kv-stats-mappings.erl"
          "${kv_stats_mapping_src}"
          "${kv_stats_mapping_target}"
  DEPENDS "${kv_stats_mapping_src}")

ADD_CUSTOM_TARGET (ns_distclean)
ADD_DEPENDENCIES (ns_distclean ns_realclean ns_dataclean)

ADD_SUBDIRECTORY (deps)
REBAR(TARGET ns_server
  DEPENDS ns_cfg ale kv_mappings chronicle
  EUNIT
  EUNIT_DEPENDS triq meck
  CLEAN_HOOK ns_realclean)

ADD_CUSTOM_TARGET (erlang_deps DEPENDS gen_smtp ale ns_babysitter ns_couchdb chronicle)
ADD_CUSTOM_TARGET (ns_server_all DEPENDS ns_server erlang_deps)
ADD_CUSTOM_TARGET (ns_server_tests
  DEPENDS ns_server-eunit ns_couchdb-eunit ale-eunit erlang_deps)

# CMake won't let you add a build target as a dependency to a test. But you
# can add another test as a dependency. So this is a fake test that actually
# simply builds ns_server_tests target. Note, this is only needed when the
# tests are run in CMake way (make -C build/ test). Simple "make test" in
# ns_server directory calls a custom target ns_test which can properly depend
# on other targets.
ADD_TEST(ns_server_tests_build_for_idiotic_cmake
  "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}"
                     --target ns_server_tests)

MACRO (NS_SERVER_TEST)
  PARSE_ARGUMENTS (NS_SERVER_TEST "" "NAME;TEST_TARGET" "EXPLICIT" ${ARGN})

  IF (NOT NS_SERVER_TEST_NAME)
    MESSAGE (FATAL_ERROR "Need NAME")
  ENDIF()

  IF (NOT NS_SERVER_TEST_TEST_TARGET)
    MESSAGE (FATAL_ERROR "Need TEST_TARGET")
  ENDIF()

  SET (_name ${NS_SERVER_TEST_NAME})
  SET (_test_target ${NS_SERVER_TEST_TEST_TARGET})

  SET (_cmd "${CMAKE_COMMAND}" -D "ERL_EXECUTABLE=${ERL_EXECUTABLE}"
    -D "COUCHDB_BIN_DIR=${COUCHDB_BIN_DIR}"
    -D "TEST_TARGET=${_test_target}"
    -P "${PROJECT_SOURCE_DIR}/cmake_modules/do-test.cmake")

  IF (NOT NS_SERVER_TEST_EXPLICIT)
    # Test invocation
    ADD_TEST(NAME ${_name} COMMAND ${_cmd} WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
    SET_TESTS_PROPERTIES(${_name} PROPERTIES
      DEPENDS ns_server_tests_build_for_idiotic_cmake)
  ENDIF ()

  # Also create a custom target
  ADD_CUSTOM_TARGET (${_name} COMMAND ${_cmd} WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
  ADD_DEPENDENCIES (${_name} ns_server_tests)
ENDMACRO(NS_SERVER_TEST)

NS_SERVER_TEST (NAME ns_test TEST_TARGET start)
NS_SERVER_TEST (NAME ns_test_eunit TEST_TARGET start_eunit EXPLICIT)
NS_SERVER_TEST (NAME ns_test_triq TEST_TARGET start_triq EXPLICIT)

# Add custom target allowing folks to easily run the tests via
# make (and not via directly invoking ctest).
ADD_CUSTOM_TARGET(ns_ui_test COMMAND karma start karma.conf.js --single-run
         WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/priv/public/ui)

#
# Dialyzer targets
#
# Get COUCHBASE_PLT from environment, if specified
IF (DEFINED ENV{COUCHBASE_PLT})
  SET (_couchbase_plt "$ENV{COUCHBASE_PLT}")
  SET (_force_plt FORCE)
ELSE (DEFINED ENV{COUCHBASE_PLT})
  SET (_couchbase_plt "${PROJECT_SOURCE_DIR}/couchbase.plt")
ENDIF (DEFINED ENV{COUCHBASE_PLT})
GET_FILENAME_COMPONENT (_couchbase_plt "${_couchbase_plt}" REALPATH)
SET (COUCHBASE_PLT "${_couchbase_plt}" CACHE FILEPATH
  "Full path to couchbase.plt for dialyzer targets" ${_force_plt})
MESSAGE (STATUS "COUCHBASE_PLT set to ${COUCHBASE_PLT}")

SET (dialyzer_args -Wno_return -Wno_improper_lists -Wunknown)
ADD_CUSTOM_TARGET (ns_dialyzer
  COMMAND "${CMAKE_COMMAND}" -D "COUCHDB_BIN_DIR=${COUCHDB_BIN_DIR}"
  -D "DIALYZER_EXECUTABLE=${DIALYZER_EXECUTABLE}"
  -D "DIALYZER_FLAGS=${dialyzer_args}"
  -D "COUCHBASE_PLT=${COUCHBASE_PLT}"
  -P "${PROJECT_SOURCE_DIR}/cmake_modules/do-dialyzer.cmake"
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}" VERBATIM)
ADD_DEPENDENCIES (ns_dialyzer ns_server_all)

SET (obsessive_args -Wunmatched_returns -Werror_handling -Wrace_conditions
  -Wbehaviours -Wunderspecs)
ADD_CUSTOM_TARGET (ns_dialyzer_obsessive
  COMMAND "${CMAKE_COMMAND}" -D "COUCHDB_BIN_DIR=${COUCHDB_BIN_DIR}"
  -D "DIALYZER_EXECUTABLE=${DIALYZER_EXECUTABLE}"
  -D "DIALYZER_FLAGS=${obsessive_args}"
  -D "COUCHBASE_PLT=${COUCHBASE_PLT}"
  -P "${PROJECT_SOURCE_DIR}/cmake_modules/do-dialyzer.cmake"
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}" VERBATIM)
ADD_DEPENDENCIES (ns_dialyzer_obsessive ns_server_all)

#
# Install
#
INSTALL (DIRECTORY ebin DESTINATION "${INST_LIBDIR}/ns_server")

PyWrapper (SCRIPTS cbcollect_info)

INSTALL (PROGRAMS cbbrowse_logs DESTINATION bin)
INSTALL (PROGRAMS scripts/cbdump-config scripts/dump-guts DESTINATION bin)
INSTALL (PROGRAMS scripts/escript-wrapper DESTINATION bin)

# Configure custom copies of scripts for installation
SET (PREFIX "${CMAKE_INSTALL_PREFIX}")
SET (_instdir "${PROJECT_BINARY_DIR}/instfiles")
FILE (REMOVE_RECURSE "${_instdir}")
CONFIGURE_FILE (couchbase-server.sh.in "${_instdir}/couchbase-server" @ONLY)
INSTALL (PROGRAMS "${_instdir}/couchbase-server" DESTINATION bin)

SET (_etcdir "${_instdir}/etc")
FILE (MAKE_DIRECTORY "${_etcdir}")

# MacOS (at least) has a distinction between DATA_PREFIX and BIN_PREFIX, so we
# need to replace both
SET (DATA_PREFIX "${PREFIX}")
SET (BIN_PREFIX "${PREFIX}")
CONFIGURE_FILE (etc/static_config.in "${_etcdir}/couchbase/static_config" @ONLY)
INSTALL (FILES etc/hosts.cfg DESTINATION "${_etcdir}/couchbase")

SET (CONFIG_PREFIX "${PREFIX}/var/lib/couchbase/config")
CONFIGURE_FILE (etc/ssl_dist_opts.in "${_etcdir}/couchbase/ssl_dist_opts" @ONLY)

FILE (WRITE "${_etcdir}/couchbase/config" "")    # create empty file
SET (_cdbetcdir "${_etcdir}/couchdb/default.d")
FILE (MAKE_DIRECTORY "${_cdbetcdir}")
SET (COUCHBASE_DB_DIR "${CMAKE_INSTALL_PREFIX}/var/lib/couchbase/data")
CONFIGURE_FILE (etc/capi.ini.in "${_cdbetcdir}/capi.ini" @ONLY)
INSTALL (DIRECTORY "${_etcdir}/" DESTINATION etc)

# Empty directory to be "installed" multiple times with varying permissions
FILE (MAKE_DIRECTORY "${_instdir}/var")
INSTALL (DIRECTORY "${_instdir}/var/" DESTINATION var/lib)
INSTALL (DIRECTORY "${_instdir}/var/" DESTINATION var/lib/couchbase
  DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ GROUP_WRITE GROUP_EXECUTE)
INSTALL (DIRECTORY "${_instdir}/var/" DESTINATION var/lib/couchbase/logs
  DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
  GROUP_READ GROUP_WRITE GROUP_EXECUTE)

INSTALL (FILES etc/metrics_metadata.json DESTINATION "etc/couchbase/cm")
