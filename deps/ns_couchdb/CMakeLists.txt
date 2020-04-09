REBAR (TARGET ns_couchdb
  DEPENDS ale ns_server
  EUNIT
  EUNIT_DEPENDS triq meck
  CLEAN_HOOK ns_realclean)
INSTALL (DIRECTORY ebin DESTINATION "${INST_LIBDIR}/ns_couchdb")
