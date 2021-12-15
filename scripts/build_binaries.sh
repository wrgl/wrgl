#!/usr/bin/env bash
set -ex

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"

cd $DIR
rm -rf build
cp VERSION cmd/wrgl
cp VERSION cmd/wrgld

CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -a -o build/wrgl-linux-amd64/bin/wrgl github.com/wrgl/wrgl/wrgl
CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -a -o build/wrgl-linux-amd64/bin/wrgld github.com/wrgl/wrgl/wrgld
cp LICENSE build/wrgl-linux-amd64/LICENSE
cd $DIR/build
tar -czvf wrgl-linux-amd64.tar.gz wrgl-linux-amd64

cd $DIR
CGO_ENABLED=0 GOARCH=amd64 GOOS=darwin go build -a -o build/wrgl-darwin-amd64/bin/wrgl github.com/wrgl/wrgl/wrgl
CGO_ENABLED=0 GOARCH=amd64 GOOS=darwin go build -a -o build/wrgl-darwin-amd64/bin/wrgld github.com/wrgl/wrgl/wrgld
cp LICENSE build/wrgl-darwin-amd64/LICENSE
cd $DIR/build
tar -czvf wrgl-darwin-amd64.tar.gz wrgl-darwin-amd64

cd $DIR
CGO_ENABLED=0 GOARCH=arm64 GOOS=darwin go build -a -o build/wrgl-darwin-arm64/bin/wrgl github.com/wrgl/wrgl/wrgl
CGO_ENABLED=0 GOARCH=arm64 GOOS=darwin go build -a -o build/wrgl-darwin-arm64/bin/wrgld github.com/wrgl/wrgl/wrgld
cp LICENSE build/wrgl-darwin-arm64/LICENSE
cd $DIR/build
tar -czvf wrgl-darwin-arm64.tar.gz wrgl-darwin-arm64