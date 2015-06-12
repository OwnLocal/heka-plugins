#!/bin/bash

GOPATH=`pwd`/.goat/deps .goat/deps/bin/ginkgo watch -r --trace --race --notify
