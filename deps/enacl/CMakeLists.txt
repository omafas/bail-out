

IF (NOT DEFINED INST_LIBDIR)
  MESSAGE(FATAL_ERROR "INST_LIBDIR is not defined.")
ENDIF ()

IF (NOT TARGET ns_realclean)
  MESSAGE(FATAL_ERROR "ns_realclean target does not exist.")
ENDIF ()

SET (REBAR_OPTS -C "${CMAKE_CURRENT_SOURCE_DIR}/rebar2.config")

REBAR (TARGET enacl REBAR_OPTS ${REBAR_OPTS} CLEAN_HOOK ns_realclean)

SET(INSTALL_DIR "${INST_LIBDIR}/enacl")

# This assumes that rebar2 is used.
INSTALL (DIRECTORY ebin priv DESTINATION "${INSTALL_DIR}")

