#!/bin/bash

goat deps

export GOPATH=`goat env GOPATH`

pushd .goat/deps/src/github.com/mozilla-services/heka
source build.sh
popd
cp -RPfv .goat/deps/src/github.com/mozilla-services/heka/build/heka/src/ .goat/deps/src
