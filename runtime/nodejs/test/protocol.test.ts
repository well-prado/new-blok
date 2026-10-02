import { test } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { createServer } from "node:net";
import { Worker, DEFAULT_LIMITS } from "../worker.js";
import { loadProtocol, validateFrameBytes, type Frame, type ReceivedFrame } from "../protocol.js";
import { nodes } from "../../../testdata/worker/nodejs/nodes.js";

const token="synthetic-test-token-0000000000001";
const artifact=`sha256:${"a".repeat(64)}`;
async function fixture(generation="1", replayEntries=8192){
 const listener=createServer();await new Promise<void>(resolve=>listener.listen(0,"127.0.0.1",resolve));const address=listener.address();if(!address||typeof address==="string")throw new Error("test listener");await new Promise<void>(resolve=>listener.close(()=>resolve()));
 const endpoint=`127.0.0.1:${address.port}`;
 const worker=new Worker({nodes,token,principal:"app-1",capabilities:["http:synthetic"],artifactDigest:artifact,generation,address:endpoint,replayEntries});await worker.listen();
 const client=new (loadProtocol().blok.runtime.v1.Worker)(endpoint,grpc.credentials.createInsecure());
 const hello={protocol:"blok.runtime",major:1,minor:0,artifactDigest:artifact,catalogDigest:worker.catalog.catalogDigest,generation,capabilities:["http:synthetic"],limits:DEFAULT_LIMITS};
 const connect=(auth=token,principal="app-1")=>{const metadata=new grpc.Metadata();metadata.set("authorization",`Bearer ${auth}`);metadata.set("x-blok-principal",principal);const stream=client.Connect(metadata);stream.on("error",()=>{});return stream;};
 return {worker,client,hello,connect,close(){client.close();worker.close();}};
}
function receive(stream:grpc.ClientDuplexStream<Frame,ReceivedFrame>):Promise<ReceivedFrame>{return new Promise((resolve,reject)=>{const timer=setTimeout(()=>done(new Error("test response timeout")),2000);const done=(err?:unknown,value?:ReceivedFrame)=>{clearTimeout(timer);stream.off("data",data);stream.off("error",error);if(err)reject(err);else resolve(value!);};const data=(value:ReceivedFrame)=>done(undefined,value);const error=(err:unknown)=>done(err);stream.once("data",data);stream.once("error",error);});}
const call=(id:string,generation="1")=>({callId:id,attemptId:`attempt-${id}`,node:"fixture/echo",nodeVersion:"1.0.0",generation,deadlineUnixNanos:(BigInt(Date.now()+1000)*1000000n).toString(),input:Buffer.from('{"value":"1"}'),idempotencyKey:"operation",principal:"app-1",capabilities:["http:synthetic"]});
const status=(expected:grpc.status)=>(error:unknown)=>typeof error==="object"&&error!==null&&"code" in error&&error.code===expected;
test("actual gRPC rejects bad authentication, negotiation and stale generations",async()=>{
 for(const failure of ["token","principal","catalog","generation"]){const f=await fixture();try{const s=f.connect(failure==="token"?"wrong":token,failure==="principal"?"spoof":"app-1");const result=receive(s);s.write({hello:{...f.hello,...(failure==="catalog"?{catalogDigest:`sha256:${"b".repeat(64)}`}:{ }),...(failure==="generation"?{generation:"2"}:{})}});await assert.rejects(result,status(failure==="token"||failure==="principal"?grpc.status.UNAUTHENTICATED:grpc.status.FAILED_PRECONDITION));}finally{f.close();}}
 const f=await fixture("2");try{const s=f.connect();let result=receive(s);s.write({hello:f.hello});assert.equal((await result).ready?.contract?.generation,"2");result=receive(s);s.write({call:call("stale","1")});await assert.rejects(result,status(grpc.status.FAILED_PRECONDITION));}finally{f.close();}
});
test("worker independently rejects replay and bounded identity saturation",async()=>{
 for(const duplicate of [true,false]){const f=await fixture("1",2);try{const s=f.connect();let result=receive(s);s.write({hello:f.hello});await result;result=receive(s);s.write({call:call("one")});assert.equal((await result).result?.output.toString(),'{"value":"1"}');result=receive(s);s.write({call:call(duplicate?"one":"two")});await assert.rejects(result,status(duplicate?grpc.status.ALREADY_EXISTS:grpc.status.RESOURCE_EXHAUSTED));}finally{f.close();}}
});
test("actual deadline and Cancel terminate cooperative nodes without publishing output",async()=>{
 const f=await fixture();try{const s=f.connect();let result=receive(s);s.write({hello:f.hello});await result;
  for(const mode of ["deadline","cancel"]){const c={...call(mode),node:"fixture/slow",input:Buffer.from('{"milliseconds":500}'),deadlineUnixNanos:(BigInt(Date.now()+(mode==="deadline"?30:1000))*1000000n).toString()};result=receive(s);s.write({call:c});if(mode==="cancel")s.write({cancel:{callId:c.callId,attemptId:c.attemptId,generation:"1"}});const response=(await result).result;assert.equal(response?.error?.class,mode==="deadline"?"DEADLINE_EXCEEDED":"CANCELED");assert.equal(response?.output.length,0);}
  result=receive(s);s.write({call:call("after-cancel")});assert.equal((await result).result?.output.toString(),'{"value":"1"}');
 }finally{f.close();}
});
test("wire rejects multiple envelopes and malformed length before decoding",()=>{
 for(const bytes of [Buffer.alloc(0),Buffer.from([10,1]),Buffer.from([10,0,18,0]),Buffer.from([58,0]),Buffer.from([10,128,128,128,128,128])])assert.throws(()=>validateFrameBytes(bytes));
 assert.doesNotThrow(()=>validateFrameBytes(Buffer.from([10,0])));
});
