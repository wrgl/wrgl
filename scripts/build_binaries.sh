#!/usr/bin/env bash
set -ex

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd $DIR

rm -rf build

CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -a -o build/wrgl-linux-amd64/bin/wrgl github.com/wrgl/core/wrgl
cp LICENSE build/wrgl-linux-amd64/LICENSE
tar -czvf build/wrgl-linux-amd64.tar.gz build/wrgl-linux-amd64

CGO_ENABLED=0 GOARCH=amd64 GOOS=darwin go build -a -o build/wrgl-darwin-amd64/bin/wrgl github.com/wrgl/core/wrgl
cp LICENSE build/wrgl-darwin-amd64/LICENSE
tar -czvf build/wrgl-darwin-amd64.tar.gz build/wrgl-darwin-amd64