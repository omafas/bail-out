GoModBuild (TARGET ns_minify PACKAGE ns_server_minify
  GOVERSION 1.16.6
  OUTPUT minify)

REMOVE_SANITIZERS(ns_minify)
