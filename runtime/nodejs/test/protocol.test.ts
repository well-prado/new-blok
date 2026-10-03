import { test } from "node:test";
import assert from "node:assert/strict";
import * as grpc from "@grpc/grpc-js";
import { createServer } from "node:net";
import { Worker, DEFAULT_LIMITS } from "../worker.js";
import { loadProtocol, validateFrameBytes, type Frame, type ReceivedFrame } from "../protocol.js";
import { nodes } from "../../../testdata/worker/nodejs/nodes.js";
import { defineNode, type AnyNode } from "../../../sdk/nodejs/index.js";

const token="synthetic-test-token-0000000000001";
const artifact=`sha256:${"a".repeat(64)}`;
async function fixture(generation="1", replayEntries=8192,extra:readonly AnyNode[]=[],caps:readonly string[]=["http:synthetic"]){
 const listener=createServer();await new Promise<void>(resolve=>listener.listen(0,"127.0.0.1",resolve));const address=listener.address();if(!address||typeof address==="string")throw new Error("test listener");await new Promise<void>(resolve=>listener.close(()=>resolve()));
 const endpoint=`127.0.0.1:${address.port}`;
 const worker=new Worker({nodes:[...nodes,...extra],token,principal:"app-1",capabilities:caps,artifactDigest:artifact,generation,address:endpoint,replayEntries});await worker.listen();
 const client=new (loadProtocol().blok.runtime.v1.Worker)(endpoint,grpc.credentials.createInsecure());
 const hello={protocol:"blok.runtime",major:1,minor:0,artifactDigest:artifact,catalogDigest:worker.catalog.catalogDigest,generation,capabilities:[...caps],limits:DEFAULT_LIMITS};
 const connect=(auth=token,principal="app-1")=>{const metadata=new grpc.Metadata();metadata.set("authorization",`Bearer ${auth}`);metadata.set("x-blok-principal",principal);const stream=client.Connect(metadata);stream.on("error",()=>{});return stream;};
 return {worker,client,hello,connect,close(){client.close();worker.close();}};
}
function receive<T>(stream:grpc.ClientDuplexStream<T,ReceivedFrame>):Promise<ReceivedFrame>{return new Promise((resolve,reject)=>{const timer=setTimeout(()=>done(new Error("test response timeout")),2000);const done=(err?:unknown,value?:ReceivedFrame)=>{clearTimeout(timer);stream.off("data",data);stream.off("error",error);if(err)reject(err);else resolve(value!);};const data=(value:ReceivedFrame)=>done(undefined,value);const error=(err:unknown)=>done(err);stream.once("data",data);stream.once("error",error);});}
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
  for(const mode of ["deadline","cancel"]){const c={...call(mode),node:"fixture/slow",input:Buffer.from('{"milliseconds":500}'),deadlineUnixNanos:(BigInt(Date.now()+(mode==="deadline"?30:1000))*1000000n).toString()};result=receive(s);s.write({call:c});if(mode==="cancel")s.write({cancel:{callId:c.callId,attemptId:c.attemptId,generation:"1"}});const response=(await result).result;assert.equal(response?.error?.class,mode==="deadline"?"DEADLINE_EXCEEDED":"CANCELED");assert.equal(response?.error?.idempotencyKey,"operation");assert.equal(response?.output.length,0);}
  result=receive(s);s.write({call:call("after-cancel")});assert.equal((await result).result?.output.toString(),'{"value":"1"}');
 }finally{f.close();}
});
test("CPU-bound node cannot publish successful output beyond absolute deadline",async()=>{
 let executions=0;
 const blocked=defineNode<Record<string,never>,{done:boolean},null>({name:"fixture/blocked",version:"1.0.0",description:"Synthetic blocked event loop",input:{type:"object"},output:{type:"object",properties:{done:{type:"boolean"}},required:["done"]},deterministic:true,dependencies:null,execute(){executions++;const until=Date.now()+100;while(Date.now()<until){/* deliberately blocks timer */}return {done:true};}});
 const f=await fixture("1",8192,[blocked]);try{const s=f.connect();let result=receive(s);s.write({hello:f.hello});await result;
  result=receive(s);s.write({call:{...call("blocked"),node:"fixture/blocked",input:Buffer.from("{}"),deadlineUnixNanos:(BigInt(Date.now()+30)*1000000n).toString()}});
  const response=(await result).result;assert.equal(executions,1);assert.equal(response?.error?.class,"DEADLINE_EXCEEDED");assert.equal(response?.output.length,0);assert.equal(response?.error?.idempotencyKey,"operation");
  result=receive(s);s.write({call:call("after-blocked")});assert.equal((await result).result?.output.toString(),'{"value":"1"}');
 }finally{f.close();}
});
test("shared Go bounds accept 128 capabilities, blob references and a long valid deadline",async()=>{
 const caps=Array.from({length:128},(_,i)=>`synthetic:${i}`),f=await fixture("1",8192,[],caps);try{const s=f.connect();let result=receive(s);s.write({hello:f.hello});await result;
  result=receive(s);s.write({call:{...call("shared-bounds"),idempotencyKey:"order 123/ação",capabilities:caps,blobs:Array.from({length:128},()=>({digest:artifact,size:"0"})),deadlineUnixNanos:(BigInt(Date.now()+90000)*1000000n).toString()}});
  assert.equal((await result).result?.output.toString(),'{"value":"1"}');
 }finally{f.close();}
});
test("deadline exhausted in transit is terminal without poisoning the channel",async()=>{
 const f=await fixture();try{const s=f.connect();let result=receive(s);s.write({hello:f.hello});await result;
  result=receive(s);s.write({call:{...call("expired"),deadlineUnixNanos:(BigInt(Date.now()-10)*1000000n).toString()}});assert.equal((await result).result?.error?.class,"DEADLINE_EXCEEDED");
  result=receive(s);s.write({call:call("after-expired")});assert.equal((await result).result?.output.toString(),'{"value":"1"}');
 }finally{f.close();}
});
test("actual gRPC preserves replacement characters and 128-byte multibyte keys, rejects 129 bytes",async()=>{
 let effects=0;
 const probe=defineNode<Record<string,never>,{key:string},null>({name:"fixture/key-probe",version:"1.0.0",description:"Synthetic operation key probe",input:{type:"object"},output:{type:"object",properties:{key:{type:"string"}},required:["key"]},dependencies:null,execute(ctx){effects++;return {key:ctx.idempotencyKey};}});
 const f=await fixture("1",8192,[probe]);try{
  const s=f.connect();let result=receive(s);s.write({hello:f.hello});await result;
  for(const [i,key] of ["\ufffd","é".repeat(64)].entries()){
   result=receive(s);s.write({call:{...call(`key-${i}`),node:"fixture/key-probe",input:Buffer.from("{}"),idempotencyKey:key}});
   assert.equal(JSON.parse((await result).result!.output.toString()).key,key);
  }
  assert.equal(effects,2);
  result=receive(s);s.write({call:{...call("key-too-long"),node:"fixture/key-probe",input:Buffer.from("{}"),idempotencyKey:"é".repeat(64)+"x"}});
  await assert.rejects(result,status(grpc.status.INTERNAL));assert.equal(effects,2);
 }finally{f.close();}
});
test("actual gRPC rejects malformed raw operation keys before any node effects",async()=>{
 let effects=0;
 const probe=defineNode<Record<string,never>,{done:boolean},null>({name:"fixture/raw-key-probe",version:"1.0.0",description:"Synthetic malformed key effect counter",input:{type:"object"},output:{type:"object",properties:{done:{type:"boolean"}},required:["done"]},dependencies:null,execute(){effects++;return {done:true};}});
 for(const bad of [Buffer.from([0xff]),Buffer.from([0xc0,0xaf]),Buffer.from([0xed,0xa0,0x80]),Buffer.from([0xe2,0x82])]){
  const f=await fixture("1",8192,[probe]);try{
   const service=loadProtocol().blok.runtime.v1.Worker.service.Connect;
   const metadata=new grpc.Metadata();metadata.set("authorization",`Bearer ${token}`);metadata.set("x-blok-principal","app-1");
   const stream=f.client.makeBidiStreamRequest<Buffer,ReceivedFrame>("/blok.runtime.v1.Worker/Connect",value=>value,value=>service.responseDeserialize(value),metadata);
   const ready=receive(stream);
   stream.write(service.requestSerialize({hello:f.hello}));assert.ok((await ready).ready);
   const key="k".repeat(bad.length),bytes=service.requestSerialize({call:{...call("raw-key"),node:"fixture/raw-key-probe",input:Buffer.from("{}"),idempotencyKey:key}});
   const at=bytes.indexOf(Buffer.concat([Buffer.from([50,bad.length]),Buffer.from(key)]));assert.ok(at>=0);bad.copy(bytes,at+2);
   const rejected=new Promise<grpc.ServiceError>((resolve,reject)=>{const timer=setTimeout(()=>{stream.cancel();reject(new Error("malformed key not rejected"));},2000);stream.once("error",(error:grpc.ServiceError)=>{clearTimeout(timer);resolve(error);});});
   stream.write(bytes);assert.equal((await rejected).code,grpc.status.INTERNAL);assert.equal(effects,0);
  }finally{f.close();}
 }
});
test("raw key scanner rejects shadowed invalid keys and malformed field bounds",()=>{
 const frame=(body:Buffer)=>Buffer.concat([Buffer.from([26,body.length]),body]);
 for(const body of [Buffer.from([50,1,255,50,1,97]),Buffer.from([50,2,97]),Buffer.from([48,0]),Buffer.from([0]),Buffer.from([58,128,128,128,128,128]),Buffer.from([56,128,128,128,128,128,128,128,128,128,2]),Buffer.from([61,0])])assert.throws(()=>validateFrameBytes(frame(body)));
 assert.doesNotThrow(()=>validateFrameBytes(frame(Buffer.from([50,3,239,191,189]))));
});
test("wire rejects multiple envelopes and malformed length before decoding",()=>{
 for(const bytes of [Buffer.alloc(0),Buffer.from([10,1]),Buffer.from([10,0,18,0]),Buffer.from([58,0]),Buffer.from([10,128,128,128,128,128])])assert.throws(()=>validateFrameBytes(bytes));
 assert.doesNotThrow(()=>validateFrameBytes(Buffer.from([10,0])));
});
test("actual paused socket consumer fails closed at bounded outbound bytes",async()=>{
 let calls=0;
 const bulk=defineNode<Record<string,never>,{data:string},null>({name:"fixture/bulk",version:"1.0.0",description:"Synthetic bounded slow-consumer workload",input:{type:"object"},output:{type:"object",properties:{data:{type:"string"}},required:["data"]},deterministic:true,dependencies:null,execute(){calls++;return {data:"x".repeat(800000)};}});
 const f=await fixture("1",8192,[bulk]);try{
  const s=f.connect();const ready=receive(s);s.write({hello:f.hello});await ready;s.pause();
  const rejected=new Promise<grpc.ServiceError>((resolve,reject)=>{const timer=setTimeout(()=>reject(new Error("slow consumer was not bounded")),8000);s.once("error",(error:grpc.ServiceError)=>{clearTimeout(timer);resolve(error);});});
  for(let i=0;i<32;i++)s.write({call:{...call(`bulk-${i}`),node:"fixture/bulk",input:Buffer.from("{}"),deadlineUnixNanos:(BigInt(Date.now()+10000)*1000000n).toString()}});
  // Resume after a controlled pause. HTTP/2 status
  // itself can be flow-controlled behind the paused response messages.
  await new Promise(resolve=>setTimeout(resolve,1500));s.resume();const error=await rejected;
  assert.equal(error.code,grpc.status.RESOURCE_EXHAUSTED);assert.ok(calls>0 && calls<=32);
 }finally{f.close();}
});
test("actual socket rejects truncated and oversized serialized messages",async()=>{
 for(const fixtureBytes of [Buffer.from([10,1]),Buffer.alloc((1<<20)+1)]){
  const f=await fixture();try{const metadata=new grpc.Metadata();metadata.set("authorization",`Bearer ${token}`);metadata.set("x-blok-principal","app-1");
   const stream=f.client.makeBidiStreamRequest<Buffer,ReceivedFrame>("/blok.runtime.v1.Worker/Connect",value=>value,value=>loadProtocol().blok.runtime.v1.Worker.service.Connect.responseDeserialize(value),metadata);
   const error=await new Promise<grpc.ServiceError>((resolve,reject)=>{const timer=setTimeout(()=>{stream.cancel();reject(new Error("malformed message not rejected"));},2000);stream.on("error",(error:grpc.ServiceError)=>{clearTimeout(timer);resolve(error);});stream.write(fixtureBytes);});
   assert.equal(error.code,fixtureBytes.length>1<<20?grpc.status.RESOURCE_EXHAUSTED:grpc.status.INTERNAL);
  }finally{f.close();}
 }
});
