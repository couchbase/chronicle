# Used when built as part of couchbase server only.

IF (NOT DEFINED INST_LIBDIR)
  MESSAGE(FATAL_ERROR "INST_LIBDIR is not defined.")
ENDIF ()

IF (NOT TARGET ns_realclean)
  MESSAGE(FATAL_ERROR "ns_realclean target does not exist.")
ENDIF ()

REBAR (TARGET chronicle CLEAN_HOOK ns_realclean)

# This assumes that rebar2 is used.
INSTALL (DIRECTORY ebin priv DESTINATION "${INST_LIBDIR}/chronicle")
