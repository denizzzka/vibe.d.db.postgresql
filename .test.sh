#!/bin/bash
set -ev

dub upgrade # because "Non-optional dependency libasync of vibe-d:core not found in dependency tree!?."
dub test
dub run --config=integration_test -- --conninfo="${1}" --debug=true
dub build :example

#if [[ ${DC} -eq "dmd" ]]; then dub run dscanner -- -s; fi #disabled due to stall
#if [[ ${DC} -eq "dmd" ]]; then dub run dscanner -- -S; fi #disabled due to assertion failure in dsymbol
