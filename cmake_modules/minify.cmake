GET_FILENAME_COMPONENT(app_min_js_file_name ${APP_MIN_FILE} NAME)
GET_FILENAME_COMPONENT(index_min_html_file_name ${INDEX_MIN_FILE} NAME)

MESSAGE(STATUS "Building ${app_min_js_file_name} and ${index_min_html_file_name} ...")

EXECUTE_PROCESS(
  COMMAND deps/gocode/bin/minify --index-html ${INDEX_HTML_FILE} --min-js-name ${app_min_js_file_name} --min-html-name ${index_min_html_file_name}
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

IF(result)
  MESSAGE( FATAL_ERROR "Failed to build ${app_min_js_file_name} or ${index_min_html_file_name}. Check build logs." )
ENDIF(result)
