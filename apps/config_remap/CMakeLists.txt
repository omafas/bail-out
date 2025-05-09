REBAR (TARGET config_remap
       DEPENDS ale chronicle
        # Some misc functions used by script that live in ns_server
        ns_server
        # For cb_gosecrets_runner
        ns_babysitter
       CLEAN_HOOK ns_realclean)
REBAR (TARGET config_remap_script
       COMMAND escriptize
       DEPENDS config_remap
       NOCLEAN)
INSTALL (PROGRAMS config_remap DESTINATION bin)
