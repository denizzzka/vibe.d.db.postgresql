#!/bin/bash
set -ev

dub test
dub run --config=integration_test -- --conninfo="${1}"

if [[ ${DC} -eq "dmd" ]]; then dub run dscanner -- -s; fi
#if [[ ${DC} -eq "dmd" ]]; then dub run dscanner -- -S; fi #disabled due to assertion failure in dsymbol
