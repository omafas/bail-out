GoModBuild (TARGET ns_minify_js PACKAGE ns_server_minify_js
  GOVERSION SUPPORTED_NEWER
  OUTPUT minify_js)

REMOVE_SANITIZERS(ns_minify_js)
