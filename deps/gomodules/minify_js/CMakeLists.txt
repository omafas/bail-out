GoModBuild (TARGET ns_minify_js PACKAGE ns_server_minify_js
  GOVERSION 1.16.6
  OUTPUT minify_js)

REMOVE_SANITIZERS(ns_minify_js)
