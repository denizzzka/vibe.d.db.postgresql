#!/bin/bash
set -ev

dub test
dub run --config=integration_test -- --conninfo="${1}" --debug=true
dub run --config=integration_test_vibe_core -- --conninfo="${1}" --debug=true
dub build :example

#if [[ ${DC} -eq "dmd" ]]; then dub run dscanner -- -s; fi #disabled due to stall
#if [[ ${DC} -eq "dmd" ]]; then dub run dscanner -- -S; fi #disabled due to assertion failure in dsymbol
