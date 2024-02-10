REBAR (TARGET ns_babysitter
  DEPENDS ale
  EUNIT
  EUNIT_DEPENDS triq meck
  CLEAN_HOOK ns_realclean)
INSTALL (DIRECTORY ebin DESTINATION "${INST_LIBDIR}/ns_babysitter")
