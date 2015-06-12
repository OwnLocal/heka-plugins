#!/bin/bash

goat deps

export GOPATH=`goat env GOPATH`

pushd .goat/deps/src/github.com/mozilla-services/heka
source build.sh
popd
cp -RP .goat/deps/src/github.com/mozilla-services/heka/build/heka/src/ .goat/deps/src
