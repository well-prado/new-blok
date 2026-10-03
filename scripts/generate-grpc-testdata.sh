#!/bin/sh
# Pinned local generation of trigger/grpc's synthetic test service, with the
# same toolchain as scripts/generate-runtime.sh.
set -eu
export PATH="/usr/local/go/bin:/go/bin:$PATH"
go install github.com/bufbuild/buf/cmd/buf@v1.57.2
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
cd trigger/grpc/internal/orderpb
buf generate
