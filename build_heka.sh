#!/bin/bash

: ${CIRCLE_BUILD_NUM:=`git describe --always`}
: ${CIRCLE_ARTIFACTS:=`pwd`}

goat deps

export GOPATH=`goat env GOPATH`

rsync -a --exclude-from .gitignore `pwd`/ .goat/deps/src/github.com/mozilla-services/heka/externals/heka-plugins/

pushd .goat/deps/src/github.com/mozilla-services/heka
echo 'add_external_plugin(git https://github.com/OwnLocal/heka-plugins :local)' > cmake/plugin_loader.cmake
source build.sh
cpack -G TGZ -D CPACK_PACKAGE_FILE_NAME=heka-${CIRCLE_BUILD_NUM}
cp heka-${CIRCLE_BUILD_NUM}.tar.gz ${CIRCLE_ARTIFACTS}/
popd

