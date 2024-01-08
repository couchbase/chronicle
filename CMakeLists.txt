# Used when built as part of couchbase server only.

if(NOT DEFINED REBAR_VERSION)
  # Default to building with rebar2 (the old behaviour)
  SET(REBAR_VERSION "rebar2")
endif()

IF (${REBAR_VERSION} STREQUAL "rebar2")
  # Config override only needed for rebar2
  SET (REBAR_OPTS -C "${CMAKE_CURRENT_SOURCE_DIR}/rebar2.config")
ENDIF()

IF (NOT DEFINED INST_LIBDIR)
  MESSAGE(FATAL_ERROR "INST_LIBDIR is not defined.")
ENDIF ()

IF (NOT TARGET ns_realclean)
  MESSAGE(FATAL_ERROR "ns_realclean target does not exist.")
ENDIF ()

REBAR (TARGET chronicle REBAR_OPTS ${REBAR_OPTS} CLEAN_HOOK ns_realclean)
REBAR (TARGET chronicle_dump REBAR_OPTS ${REBAR_OPTS}
       COMMAND escriptize NOCLEAN DEPENDS chronicle)

SET(INSTALL_DIR "${INST_LIBDIR}/chronicle")

IF (${REBAR_VERSION} STREQUAL "rebar2")
  INSTALL (DIRECTORY ebin DESTINATION "${INSTALL_DIR}")
  INSTALL (PROGRAMS chronicle_dump DESTINATION bin)
ELSE()
  INSTALL (DIRECTORY _build/default/lib/chronicle/ebin DESTINATION "${INSTALL_DIR}")
  INSTALL (PROGRAMS _build/default/bin/chronicle_dump DESTINATION bin)
ENDIF()

# priv/ only exists on Linux currently, so make this OPTIONAL
INSTALL (DIRECTORY priv OPTIONAL DESTINATION "${INSTALL_DIR}")
