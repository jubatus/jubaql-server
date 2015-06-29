#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: increase-version.sh toversion"
    exit 1
fi

OLDVERSION=$(grep "version := " processor/build.sbt | sed 's/[^"]*"\([^"]*\).*/\1/')
NEWVERSION=$1

echo "Bumping version from $OLDVERSION to $NEWVERSION ..."

sed -i "s/$OLDVERSION/$NEWVERSION/g" */build.sbt

sed -i "s/$OLDVERSION/$NEWVERSION/g" README.md

echo "Checking for old occurences of $OLDVERSION ..."

grep -F -R "$OLDVERSION" */src
grep -F --directories=skip "$OLDVERSION" */*
grep -F --directories=skip "$OLDVERSION" *

