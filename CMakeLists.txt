CMAKE_MINIMUM_REQUIRED (VERSION 3.15)

PROJECT (ns_server)
INCLUDE (FindCouchbaseErlang)
INCLUDE (FindCouchbaseGo)
INCLUDE (CouchbasePlugInUI)
INCLUDE (ParseArguments)
INCLUDE (CTest)
INCLUDE (FindCouchbaseLibsodium)

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

IF (BUILD_COLUMNAR)
  SET (_ns_version "${COLUMNAR_PRODUCT_VERSION}-enterprise-analytics")
ELSEIF (BUILD_ENTERPRISE)
  SET (_ns_version "${_product_version}-enterprise")
ELSE ()
  SET (_ns_version "${_product_version}-community")
ENDIF ()

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
# Create "cluster_run" and "cluster_connect" scripts in the binary
# directory that use the shipped Python interpreter
ConfigureWrapper(
  "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_INSTALL_PREFIX}/lib"
  "${CBPY_INSTALL}" "${CMAKE_CURRENT_BINARY_DIR}/cluster_run"
)
ConfigureWrapper(
  "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_INSTALL_PREFIX}/lib"
  "${CBPY_INSTALL}" "${CMAKE_CURRENT_BINARY_DIR}/cluster_connect"
)

GET_FILENAME_COMPONENT (_couchdb_src "${COUCHDB_SRC_DIR}" REALPATH)
GET_FILENAME_COMPONENT (_couchdb_bin "${COUCHDB_DEFAULT_LIB_DIR}" REALPATH)
FILE (WRITE "${PROJECT_BINARY_DIR}/config.mk"
  "COUCHDB_SRC_DIR = ${_couchdb_src}\n"
  "COUCHDB_BIN_DIR = ${_couchdb_bin}\n")

#build UI
IF (BUILD_COLUMNAR)
  SET (NS_UI_SRC "${PROJECT_SOURCE_DIR}/../columnar-ui-v1/src/ui")
ELSE (BUILD_COLUMNAR)
  SET (NS_UI_SRC "${PROJECT_SOURCE_DIR}/priv/public/ui")
ENDIF (BUILD_COLUMNAR)
SET (NS_UI_STAMP "${NS_UI_BUILD_DIR}/ns")
SET (NS_UI_BIN "${NS_UI_PUB_DIR}/ui")
SET (NS_UI_PUB_SRC "${NS_UI_SRC}/..")

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
  COMMAND "${PROJECT_BINARY_DIR}/deps/gocode/minify_js"
  "--in-dir=${NS_UI_PUB_DIR}"
  "--out-dir=${NS_UI_PUB_JS_OUT_DIR}"
  "--importmap-path=${NS_UI_SRC}/importmap.json"
  COMMAND "${CMAKE_COMMAND}" -E touch "${NS_UI_BUILD_DIR}/out.js.stamp"
  DEPENDS ns_minify_js
  DEPENDS "${_out_js_stamp_depends}"
  VERBATIM)
ADD_CUSTOM_COMMAND (OUTPUT "${NS_UI_BUILD_DIR}/out.css.stamp"
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${NS_UI_PUB_CSS_OUT_DIR}"
  COMMAND "${PROJECT_BINARY_DIR}/deps/gocode/minify_css"
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

FILE (GLOB NS_UI_ICONS_IMGS_ETC "${NS_UI_PUB_SRC}/*.*")
FILE (COPY ${NS_UI_ICONS_IMGS_ETC} DESTINATION "${NS_UI_PUB_DIR}")

INSTALL (DIRECTORY "${NS_UI_PUB_SRC}/"
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
  COMMAND "${CMAKE_COMMAND}" -E remove -f apps/ns_server/rebar.config
  COMMAND "${CMAKE_COMMAND}" -E remove -f apps/ns_server/src/ns_server.app.src
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

IF (BUILD_COLUMNAR)
  SET (DEFAULT_PROFILE "analytics")
ELSE (BUILD_COLUMNAR)
  SET (DEFAULT_PROFILE "default")
ENDIF (BUILD_COLUMNAR)

SET (ns_server_app_src "${PROJECT_SOURCE_DIR}/apps/ns_server/src/ns_server.app.src")

ADD_CUSTOM_TARGET (ns_cfg DEPENDS "${ns_server_app_src}" "${rebar_config}")
ADD_CUSTOM_COMMAND(OUTPUT "${ns_server_app_src}"
  COMMAND "${CMAKE_COMMAND}" -D "INPUT_FILE=${ns_server_app_src}.in"
  -D "OUTPUT_FILE=${ns_server_app_src}"
  -D "SEARCH=NS_SERVER_VERSION"
  -D "REPLACE=${NS_SERVER_VERSION}"
  -P "${PROJECT_SOURCE_DIR}/cmake_modules/string-replace.cmake"
  VERBATIM)

SET (rebar_config "${PROJECT_SOURCE_DIR}/apps/ns_server/rebar.config")
CONFIGURE_FILE ("${rebar_config}.in" "${rebar_config}" @ONLY)
SET (babysitter_rebar_config "${PROJECT_SOURCE_DIR}/apps/ns_babysitter/rebar.config")
CONFIGURE_FILE ("${babysitter_rebar_config}.in" "${babysitter_rebar_config}" @ONLY)

IF (NOT ESCRIPT_EXECUTABLE)
  MESSAGE (FATAL_ERROR "escript not found")
ENDIF (NOT ESCRIPT_EXECUTABLE)

SET (kv_stats_mapping_src "${PROJECT_SOURCE_DIR}/apps/ns_server/priv/kv_stats_mappings.txt")
SET (kv_stats_mapping_target "${PROJECT_SOURCE_DIR}/apps/ns_server/src/kv_stats_mappings.erl")
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
      DEPENDS
        couchdb
        kv_mappings
        ns_cfg
        ns_goport
        ns_gosecrets
        vbmap
      EUNIT
      CLEAN_HOOK ns_realclean)

ADD_CUSTOM_TARGET (ns_server_all DEPENDS ns_server)
ADD_CUSTOM_TARGET (ns_server_tests DEPENDS ns_server-eunit ns_server_all)

# CMake won't let you add a build target as a dependency to a test. You can,
# however, define a test as a set for a test fixture (set of tests) and it
# will be executed once before any of the tests. This is a fake test that
# actually simply builds ns_server_tests target. Note, this is only needed
# when the tests are run in CMake way (make -C build/ test). Unfortunately,
# this is the way that commit validation runs tests so this is still relevant.
# Simple "make test" in ns_server directory calls a custom target ns_test
# which can properly depend on other targets.
ADD_TEST(ns_server_tests_build_for_idiotic_cmake
  "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}"
                     --target ns_server_tests)
SET_TESTS_PROPERTIES(ns_server_tests_build_for_idiotic_cmake PROPERTIES
        FIXTURES_SETUP NS_TEST_SETUP)

LIST(APPEND NS_TEST_BINARY_DEPS vbmap gosecrets goport)
SET(NS_TEST_BINARY_DEPS ${NS_TEST_BINARY_DEPS} CACHE STRING "")

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
    -D "COUCHDB_BIN_DIR=${COUCHDB_DEFAULT_LIB_DIR}"
    -D "TEST_TARGET=${_test_target}"
    -D "TEST_FILTER=${TEST_FILTER}"
    # We pass the CCACHE DIR to be able to load anything we need from the
    # script. It otherwise wouldn't have access to those variables.
    -D "CCACHE_DIR=${CMAKE_BINARY_DIR}"
    -P "${PROJECT_SOURCE_DIR}/cmake_modules/do-test.cmake")

  IF (NOT NS_SERVER_TEST_EXPLICIT)
    # Test invocation
    ADD_TEST(NAME ${_name} COMMAND ${_cmd} WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
    SET_TESTS_PROPERTIES(${_name} PROPERTIES FIXTURES_REQUIRED NS_TEST_SETUP)
  ENDIF ()

  # Also create a custom target
  ADD_CUSTOM_TARGET (${_name} COMMAND ${_cmd} WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
  ADD_DEPENDENCIES (${_name} ns_server_tests)
ENDMACRO(NS_SERVER_TEST)

function(discover_eunit_tests)
    MESSAGE("Discovering erlang files for eunit tests")

    ## Pull out all the .erl files from ns_server (we recurse the app folders).
    ## CMake doesn't recommend using GLOB to do this, not all build systems
    ## support GLOB/CONFIGURE_DEPENDS to re-run the cmake step when we add a
    ## new file that matches the GLOB expression, but both make and ninja do
    ## and I believe those are the only ones that we use at the moment. This
    ## isn't the only place that we do this either...
    FILE(GLOB_RECURSE ERL_FILES
            CONFIGURE_DEPENDS
            apps/*.erl)

    foreach(FILE ${ERL_FILES})
        FILE(READ ${FILE} TMPTXT)

        ## We only want to run ns_test for modules that have tests as
        ## starting up erlang isn't particularly cheap (can take a couple of
        ## seconds on my M2 Macbook Pro).
        string(FIND "${TMPTXT}"
                    "-include_lib(\"eunit/include/eunit.hrl\")." MATCH)
        if (NOT ${MATCH} EQUAL -1)
            GET_FILENAME_COMPONENT(NAME_NO_EXT ${FILE} NAME_WLE)
            SET(TEST_FILTER ${NAME_NO_EXT})
            NS_SERVER_TEST (NAME ns_test_${NAME_NO_EXT} TEST_TARGET start)
        endif()
    endforeach(FILE ${ERL_FILES})

endfunction(discover_eunit_tests)

discover_eunit_tests()

NS_SERVER_TEST (NAME ns_test TEST_TARGET start)
NS_SERVER_TEST (NAME ns_test_eunit TEST_TARGET start_eunit EXPLICIT)
NS_SERVER_TEST (NAME ns_test_triq TEST_TARGET start_triq EXPLICIT)

# Add custom target allowing folks to easily run the tests via
# make (and not via directly invoking ctest).
ADD_CUSTOM_TARGET(ns_ui_test COMMAND karma start karma.conf.js --single-run
         WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/priv/public/ui)

# cbcollect_tests
ADD_CUSTOM_TARGET(ns_cbcollect_tests
  COMMAND ${PYTHON_EXE} cbcollect_test.py
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/pylib)

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
  COMMAND "${CMAKE_COMMAND}" -D "COUCHDB_BIN_DIR=${COUCHDB_DEFAULT_LIB_DIR}"
  -D "DIALYZER_EXECUTABLE=${DIALYZER_EXECUTABLE}"
  -D "DIALYZER_FLAGS=${dialyzer_args}"
  -D "COUCHBASE_PLT=${COUCHBASE_PLT}"
  -P "${PROJECT_SOURCE_DIR}/cmake_modules/do-dialyzer.cmake"
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}" VERBATIM)
ADD_DEPENDENCIES (ns_dialyzer ns_server_all)

#
# Install
#
INSTALL (DIRECTORY _build/default/lib/chronicle/ebin DESTINATION
        "${INST_LIBDIR}/chronicle")
# priv/ only exists on Linux currently, so make this OPTIONAL
INSTALL (DIRECTORY _build/default/lib/chronicle/priv OPTIONAL DESTINATION
        "${INST_LIBDIR}/chronicle")
INSTALL (PROGRAMS _build/default/bin/chronicle_dump DESTINATION bin)

INSTALL (DIRECTORY _build/default/lib/enacl/ebin
                   _build/default/lib/enacl/priv DESTINATION
        "${INST_LIBDIR}/enacl")
INSTALL (DIRECTORY _build/default/lib/esaml/ebin DESTINATION
        "${INST_LIBDIR}/esaml")
INSTALL (DIRECTORY _build/default/lib/gen_smtp/ebin DESTINATION
        "${INST_LIBDIR}/gen_smtp")
INSTALL (DIRECTORY _build/default/lib/iso8601/ebin DESTINATION
        "${INST_LIBDIR}/iso8601")
INSTALL (DIRECTORY _build/default/lib/jose/ebin DESTINATION
        "${INST_LIBDIR}/jose")
INSTALL (DIRECTORY _build/default/lib/jiffy/ebin
                   _build/default/lib/jiffy/priv DESTINATION
        "${INST_LIBDIR}/jiffy")

INSTALL (DIRECTORY _build/default/lib/ale/ebin DESTINATION
         "${INST_LIBDIR}/ale")
INSTALL (DIRECTORY _build/default/lib/ns_couchdb/ebin DESTINATION
         "${INST_LIBDIR}/ns_couchdb")
INSTALL (DIRECTORY _build/default/lib/ns_babysitter/ebin DESTINATION
         "${INST_LIBDIR}/ns_babysitter")
INSTALL (DIRECTORY _build/default/lib/ns_common/ebin DESTINATION
         "${INST_LIBDIR}/ns_common")
INSTALL (DIRECTORY _build/default/lib/ns_server/ebin DESTINATION
         "${INST_LIBDIR}/ns_server")
INSTALL (DIRECTORY _build/default/lib/cb_dets/ebin DESTINATION
         "${INST_LIBDIR}/cb_dets")

INSTALL (DIRECTORY _build/default/lib/config_remap/ebin DESTINATION
         "${INST_LIBDIR}/config_remap")
INSTALL (PROGRAMS _build/default/bin/config_remap DESTINATION bin)

INSTALL(FILES
        pylib/installed_script_helpers.py
        scripts/node_remap.py
        DESTINATION lib/python)

INSTALL(FILES
        pylib/gosecrets_client_lib.py
        DESTINATION lib/python)

PyWrapper (SCRIPTS cbcollect_info)
PyWrapper (SCRIPTS scripts/node_remap)
PyWrapper (SCRIPTS scripts/dump-keys)
PyWrapper (SCRIPTS scripts/dump-bucket-deks)

INSTALL (PROGRAMS cbbrowse_logs DESTINATION bin)
INSTALL (PROGRAMS scripts/cbdump-config scripts/dump-guts DESTINATION bin)
INSTALL (PROGRAMS scripts/escript-wrapper DESTINATION bin)

# Configure custom copies of scripts for installation
SET (PREFIX "${CMAKE_INSTALL_PREFIX}")
SET (_instdir "${PROJECT_BINARY_DIR}/instfiles")
FILE (REMOVE_RECURSE "${_instdir}")
CONFIGURE_FILE (couchbase-server.sh.in "${_instdir}/couchbase-server" @ONLY)
INSTALL (PROGRAMS "${_instdir}/couchbase-server" DESTINATION bin)
INSTALL (FILES "validate-cpu-microarchitecture.sh" DESTINATION bin)

SET (_etcdir "${_instdir}/etc")
FILE (MAKE_DIRECTORY "${_etcdir}")

# MacOS (at least) has a distinction between DATA_PREFIX and BIN_PREFIX, so we
# need to replace both
SET (DATA_PREFIX "${PREFIX}")
SET (BIN_PREFIX "${PREFIX}")
CONFIGURE_FILE (etc/static_config.in "${_etcdir}/couchbase/static_config" @ONLY)

# Config profiles
IF (BUILD_COLUMNAR)
  # The analytics profile is the default profile for enterprise analytics builds
  CONFIGURE_FILE (etc/analytics_profile "${_etcdir}/couchbase/analytics_profile" @ONLY)
  CONFIGURE_FILE (etc/analytics_provisioned_profile "${_etcdir}/couchbase/analytics_provisioned_profile" @ONLY)
ELSE (BUILD_COLUMNAR)
  CONFIGURE_FILE (etc/default_profile "${_etcdir}/couchbase/default_profile" @ONLY)
  CONFIGURE_FILE (etc/serverless_profile "${_etcdir}/couchbase/serverless_profile" @ONLY)
  CONFIGURE_FILE (etc/provisioned_profile "${_etcdir}/couchbase/provisioned_profile" @ONLY)
ENDIF (BUILD_COLUMNAR)

INSTALL (FILES etc/hosts.cfg DESTINATION "${_etcdir}/couchbase")
INSTALL (PROGRAMS etc/create-provisioned-cgroups.sh DESTINATION bin)

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

AddMetricsMetadata (JSON etc/metrics_metadata.json COMPONENT cm)

ADD_SUBDIRECTORY(cluster_tests)
