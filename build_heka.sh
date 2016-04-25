#!/bin/bash

: ${CIRCLE_BUILD_NUM:=`git describe --always`}
: ${CIRCLE_ARTIFACTS:=`pwd`}
: ${BUILD_TYPE:=TGZ}

goat deps

export GOPATH=`goat env GOPATH`

mkdir -p .goat/deps/src/github.com/mozilla-services/heka/externals
rsync -a --exclude-from .gitignore `pwd`/ .goat/deps/src/github.com/mozilla-services/heka/externals/heka-plugins/

pushd .goat/deps/src/github.com/mozilla-services/heka
echo 'add_external_plugin(git https://github.com/OwnLocal/heka-plugins :local)' > cmake/plugin_loader.cmake
echo 'add_external_plugin(git https://github.com/OwnLocal/heka-s3 goamzfix)' >> cmake/plugin_loader.cmake
echo 'git_clone(https://github.com/hhkbp2/go-strftime d82166ec6782f870431668391c2e321069632fe7)' >> cmake/plugin_loader.cmake

source build.sh
BUILD_NAME=heka_0.9.2-${CIRCLE_BUILD_NUM}_amd64
cpack -G ${BUILD_TYPE} -D CPACK_PACKAGE_FILE_NAME=${BUILD_NAME} -D CPACK_DEBIAN_PACKAGE_VERSION=0.9.2-${CIRCLE_BUILD_NUM}
cp ${BUILD_NAME}.* ${CIRCLE_ARTIFACTS}/
popd
