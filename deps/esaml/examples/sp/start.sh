#!/bin/bash

HOST="${host:=127.0.0.1}"
APP_NAME="esaml_example_sp"
COOKIE="${APP_NAME}"
NODE_NAME="${APP_NAME}@${HOST}"
UNAME_STR=`uname`
if [[ "${UNAME_STR}" == 'Linux' || "${UNAME_STR}" == 'Darwin' ]]; then
    EXE_NAME=erl
else
    EXE_NAME='start //MAX werl.exe'
    #exename='erl.exe'
fi

# Node name
NODE_NAME_OPT="-name ${NODE_NAME}"

# Cookie
COOKIE_OPT="-setcookie ${COOKIE}"

# PATHS
PATHS_OPT="-pa"
PATHS_OPT="${PATHS_OPT} _build/default/lib/*/ebin"
PATHS_OPT="${PATHS_OPT} _checkouts/*/ebin"

START_OPTS="${PATHS_OPT} ${COOKIE_OPT} ${NODE_NAME_OPT}"

# DDERL start options
echo "------------------------------------------"
echo "Starting ESaml Example (SP)"
echo "------------------------------------------"
echo "Node Name : ${NODE_NAME}"
echo "Cookie    : ${COOKIE}"
echo "EBIN Path : ${PATHS_OPT}"
echo "------------------------------------------"

# Starting dderl
${EXE_NAME} ${START_OPTS} -eval "application:ensure_all_started(sp)."
