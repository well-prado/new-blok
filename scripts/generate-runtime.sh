#!/bin/sh
# Pinned local generation; buf compiles the proto without a system protoc.
set -eu
export PATH="/usr/local/go/bin:/go/bin:$PATH"
go install github.com/bufbuild/buf/cmd/buf@v1.57.2
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
cd contract/runtime
buf generate
