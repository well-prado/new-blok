import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { ProtoGrpcType } from "./generated/runtime.js";
export type { Call__Output as Call } from "./generated/blok/runtime/v1/Call.js";
export type { Hello__Output as Hello } from "./generated/blok/runtime/v1/Hello.js";
export type { Frame, Frame__Output as ReceivedFrame } from "./generated/blok/runtime/v1/Frame.js";
export function repositoryRoot(): string {
  let here = dirname(fileURLToPath(import.meta.url));
  while (dirname(here) !== here) {
    if (existsSync(resolve(here, "contract/runtime/runtime.proto"))) return here;
    here = dirname(here);
  }
  throw new Error("canonical_proto_not_found");
}
export function loadProtocol(protoPath = resolve(repositoryRoot(), "contract/runtime/runtime.proto")): ProtoGrpcType {
  const root = repositoryRoot();
  const expected = readFileSync(resolve(root, "runtime/nodejs/generated/proto.sha256"), "utf8").trim();
  if (createHash("sha256").update(readFileSync(protoPath)).digest("hex") !== expected) throw new Error("proto_drift");
  const definition = loadSync(protoPath, { longs: String, enums: String, defaults: true, oneofs: true });
  const loaded = grpc.loadPackageDefinition(definition) as unknown as ProtoGrpcType;
  const connect = loaded.blok.runtime.v1.Worker.service.Connect;
  const deserialize = connect.requestDeserialize;
  connect.requestDeserialize = bytes => {
    validateFrameBytes(bytes);
    return deserialize(bytes);
  };
  return loaded;
}
// protobuf normally ignores unknown fields and accepts multiple oneof members.
// At the worker direction boundary, reject both rather than selecting the last.
export function validateFrameBytes(bytes: Buffer): void {
  const tag = bytes[0];
  if (tag === undefined || tag < 10 || tag > 50 || tag % 8 !== 2) throw new Error("invalid_frame");
  let length = 0, factor = 1, at = 1;
  for (let i = 0; i < 5; i++) {
    const byte = bytes[at++];
    if (byte === undefined) throw new Error("invalid_frame");
    length += (byte & 127) * factor;
    if (!(byte & 128)) { if (at + length !== bytes.length) throw new Error("invalid_frame"); return; }
    factor *= 128;
  }
  throw new Error("invalid_frame");
}
