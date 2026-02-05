SCRIPT_IN = purge_tombstones.sh.in
SCRIPT_OUT = purge_tombstones.sh

ERL_MODULE = purge_tombstones.erl
ERL_MODULE_STRING = purge_tombstones.erl.str

all: ${SCRIPT_OUT}

define ERL_SCRIPT
{ok, C} = file:read_file("$(ERL_MODULE)"),
io:format("~p~n", [binary_to_list(C)]),
erlang:halt(0).
endef

export ERL_SCRIPT

${ERL_MODULE_STRING}: ${ERL_MODULE}
	erl -noshell -eval "$$ERL_SCRIPT" > "$@"

define SED_SCRIPT
/^__MODULE__$$/ {
  r ${ERL_MODULE_STRING}
  d
}
s/^__GENERATED__$$/# This file is auto-generated. Do not edit./g
endef

export SED_SCRIPT

${SCRIPT_OUT}: ${SCRIPT_IN} ${ERL_MODULE_STRING}
	sed "$$SED_SCRIPT" "${SCRIPT_IN}" > "$@"
	chmod +x "$@"

.PHONY: clean
clean:
	rm -f "${SCRIPT_OUT}" "${ERL_MODULE_STRING}"

.PHONY: check_syntax
check_syntax:
	erlc +strong_validation "${ERL_MODULE}"
