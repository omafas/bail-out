GoModBuild (TARGET ns_minify_css PACKAGE ns_server_minify_css
  GOVERSION 1.18.2
  OUTPUT minify_css)

REMOVE_SANITIZERS(ns_minify_css)
