#!/bin/bash
# Call this script whenever the .ui files changed.
pushd "$(dirname "$0")"

FILES=$(find . -type f -name "*.ui")

for uipath in $FILES
do
    pypath="../dcoraid/gui/${uipath%.*}_ui.py"
    echo "$uipath -> $pypath"
    # disable flake8 for these files
    echo "# flake8: noqa" > $pypath
    echo "# This file was auto-generated with DCOR-Aid/ui/run_uic.sh." >> $pypath
    # convert ui to py
    pyuic6 "$uipath" >> "$pypath"
done

popd
