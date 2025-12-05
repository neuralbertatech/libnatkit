#!/usr/bin/env bash

SRC_DIR=$1
BUILD_DIR=$2
INSTALL_DIR=$3


if [ ! -d ${BUILD_DIR} ]
then
  mkdir -p $(dirname ${BUILD_DIR})
  cp -r ${SRC_DIR} ${BUILD_DIR}
  cd ${BUILD_DIR}
  ./configure --arch=$(uname -m) --install-deps --prefix=${INSTALL_DIR}
  make -C ${BUILD_DIR} -j 16
  make -C ${BUILD_DIR} install
else
  echo "Skipping librdkafka build (for performace reasons)"
fi
