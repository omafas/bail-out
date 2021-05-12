ADD_SUBDIRECTORY (gen_smtp)
ADD_SUBDIRECTORY (ale)

# These two need to go before any apps that have tests (ns_couchdb as of
# today).
ADD_SUBDIRECTORY (triq)
ADD_SUBDIRECTORY (meck)

ADD_SUBDIRECTORY (ns_babysitter)
ADD_SUBDIRECTORY (ns_couchdb)
ADD_SUBDIRECTORY (gocode)
ADD_SUBDIRECTORY (chronicle)
