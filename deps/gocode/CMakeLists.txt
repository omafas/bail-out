set(_package_prefix "github.com/couchbase/ns_server/deps/gocode")
set(_proj_bin_prefix "${PROJECT_BINARY_DIR}/deps/gocode")
set(_to_clean_bins)

message(STATUS "OPENSSL_CRYPTO_LIBRARY: ${OPENSSL_CRYPTO_LIBRARY}")
# Extract directory from OPENSSL_CRYPTO_LIBRARY for CGO_LIBRARY_DIRS
# Convert to absolute path to ensure rpath works correctly at runtime
get_filename_component(_openssl_lib_dir "${OPENSSL_CRYPTO_LIBRARY}" DIRECTORY)
get_filename_component(_openssl_lib_dir "${_openssl_lib_dir}" ABSOLUTE)

set(_builds
    "with_install_path ns_goport goport bin"
    "with_install_path ns_generate_cert generate_cert bin"
    "with_install_path ns_godu godu bin/priv"
    "with_install_path ns_gosecrets gosecrets bin"
    "with_install_path ns_generate_hash generate_hash bin"
    "with_install_path chunked_reader chunked_reader bin/priv"
    "with_output ns_minify_css minify_css minify_css"
    "with_output ns_minify_js minify_js minify_js")

foreach(_build ${_builds})
  string(REPLACE " " ";" _build_list "${_build}")
  list(GET _build_list 0 _install_type)
  list(GET _build_list 1 _target)
  list(GET _build_list 2 _package_name)
  list(GET _build_list 3 _install_or_output)
  set(_package "${_package_prefix}/${_package_name}")

  if(${_install_type} STREQUAL with_install_path)
    list(APPEND _to_clean_bins "${_proj_bin_prefix}/${_package_name}")
    GoModBuild (TARGET ${_target} PACKAGE ${_package}
      GOVERSION SUPPORTED_NEWER INSTALL_PATH ${_install_or_output}
      CGO_INCLUDE_DIRS "${OPENSSL_INCLUDE_DIR}"
      CGO_LIBRARY_DIRS "${_openssl_lib_dir}")
  else()
    list(APPEND _to_clean_bins "${_proj_bin_prefix}/${_install_or_output}")
    GoModBuild (TARGET ${_target} PACKAGE ${_package}
      GOVERSION SUPPORTED_NEWER OUTPUT ${_install_or_output}
      CGO_INCLUDE_DIRS "${OPENSSL_INCLUDE_DIR}"
      CGO_LIBRARY_DIRS "${_openssl_lib_dir}")
  endif()
  GET_TARGET_PROPERTY (target_exe ${_target} GO_BINARY)
  CMAKE_PATH(GET target_exe PARENT_PATH package_bin_dir)
  SET(${_package_name}_BINARY_DIR ${package_bin_dir} CACHE STRING
      "Path to ${_package_name} binary directory")
endforeach()

REMOVE_SANITIZERS(ns_minify_css)
REMOVE_SANITIZERS(ns_minify_js)

ADD_CUSTOM_TARGET (gocode-clean
  COMMAND "${CMAKE_COMMAND}" -E rm -f ${_to_clean_bins})
ADD_DEPENDENCIES (ns_realclean gocode-clean)

GoModTidySetup()
