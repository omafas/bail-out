GoModBuild (TARGET ns_minify_js PACKAGE ns_server_minify_js
  GOVERSION 1.19
  OUTPUT minify_js)

REMOVE_SANITIZERS(ns_minify_js)
