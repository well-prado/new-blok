import { test } from "node:test";
import assert from "node:assert/strict";
import { compileSchema, normalizeJSON, defineNode, discover, DomainError, redactError, parseJSON, canonicalJSON, SchemaError, type Schema } from "../../../sdk/nodejs/index.js";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { repositoryRoot } from "../protocol.js";
import { quote, echo, panic, nodes } from "../../../testdata/worker/nodejs/nodes.js";

const ctx = (): Parameters<typeof quote.invoke>[0] => ({ signal: new AbortController().signal, principal:"app-1", capabilities:[], callId:"call",attemptId:"attempt",idempotencyKey:"operation",logger:{debug(){},info(){},warn(){},error(){}} });
test("predeclared synthetic fixtures execute with exact output/error counts",async()=>{
 const fixtures=JSON.parse(readFileSync(resolve(repositoryRoot(),"testdata/worker/nodejs/fixtures.json"),"utf8")) as {id:string;node:string;input:unknown;output?:unknown;expectedOutputs:number;expectedErrors:number;expectedEffects:number}[];
 for(const f of fixtures){const node=nodes.find(n=>n.descriptor.name===f.node);assert.ok(node,f.id);let outputs=0,errors=0;try{const result=await node.invokeJSON({...ctx(),capabilities:["http:synthetic"]},canonicalJSON(f.input));outputs++;assert.equal(result,canonicalJSON(f.output),f.id);}catch(error){errors++;if(f.expectedErrors===0)throw error;}assert.equal(outputs,f.expectedOutputs,f.id);assert.equal(errors,f.expectedErrors,f.id);assert.equal(f.expectedEffects,0,"fixture dependencies never perform external effects");}
});
test("canonical Go schema corpus matches Node normalization and diagnostics", () => {
 const corpus = parseJSON(readFileSync(resolve(repositoryRoot(),"testdata/conformance/schema-corpus.json"),"utf8")) as {name:string;schema:Schema;value:unknown;normalized?:unknown;error?:string}[];
 for(const fixture of corpus){
  const schema=compileSchema(canonicalJSON(fixture.schema));
  if(fixture.error) assert.throws(()=>normalizeJSON(schema,canonicalJSON(fixture.value)),(error:unknown)=>error instanceof SchemaError && error.code===fixture.error,fixture.name);
  else assert.equal(normalizeJSON(schema,canonicalJSON(fixture.value)),canonicalJSON(fixture.normalized),fixture.name);
 }
});
test("typed nodes normalize exact integers and provider errors", async () => {
 assert.deepEqual(await quote.invoke(ctx(), {sku:"coffee",quantity:2}), {totalCents:"3000"});
 for (const n of ["9223372036854775807","-9223372036854775808"]) assert.equal(await echo.invokeJSON(ctx(), `{"value":"${n}"}`), `{"value":"${n}"}`);
 await assert.rejects(echo.invokeJSON(ctx(), `{"value":"9223372036854775808"}`));
 await assert.rejects(quote.invokeJSON(ctx(), `{"sku":"coffee","quantity":0}`));
 await assert.rejects(quote.invokeJSON(ctx(), `{"sku":"coffee","quantity":2,"unexpected":true}`));
 const error = redactError(new DomainError("provider_failure","UNCERTAIN",true,{cause:new Error("secret-value")}),"operation");
 assert.equal(error.retryable,false); assert.equal(error.idempotencyKey,"operation"); assert.ok(!JSON.stringify(error).includes("secret-value"));
 await assert.rejects(panic.invoke(ctx(),{}));
});
test("missing, null, defaults, unions, decimal and depth remain structural", () => {
 const s = compileSchema({type:"object", properties:{optional:{type:"string",nullable:true}, n:{type:"integer",wire:"int64-string",default:"1"}},required:["n"]});
 assert.equal(normalizeJSON(s,"{}"),`{"n":"1"}`); assert.equal(normalizeJSON(s,`{"optional":null}`),`{"n":"1","optional":null}`);
 assert.throws(()=>normalizeJSON(s,`{} {}`));
 assert.throws(()=>normalizeJSON(compileSchema({anyOf:[{type:"integer"},{type:"number"}]}),"1"));
 assert.equal(normalizeJSON(compileSchema({type:"number",wire:"decimal-string"}),`"123.4500"`),`"123.4500"`);
 assert.throws(()=>normalizeJSON(s,"[".repeat(66)+"0"+"]".repeat(66)));
});
test("catalog order is irrelevant and duplicate nodes are rejected", () => {
 assert.equal(discover([quote,echo]).catalogDigest,discover([echo,quote]).catalogDigest);
 assert.throws(()=>discover([quote,quote]));
});
test("required worker authority changes the canonical discovery identity",()=>{
 const make=(capabilities:readonly string[])=>defineNode<Record<string,never>,Record<string,never>,null>({name:"fixture/authority",version:"1.0.0",description:"Synthetic authority identity",input:{type:"object"},output:{type:"object"},effects:["http:synthetic"],requiredCapabilities:capabilities,dependencies:null,execute:()=>({})});
 const a=make(["http:read"]),b=make(["http:write"]);
 assert.notEqual(discover([a]).catalogDigest,discover([b]).catalogDigest);
 assert.deepEqual(a.descriptor.requiredCapabilities,["http:read"]);
 assert.throws(()=>{(a.descriptor.requiredCapabilities as string[]).push("http:write");});
 assert.equal(discover([make(["http:read","db:read"])]).catalogDigest,discover([make(["db:read","http:read"])]).catalogDigest);
 assert.throws(()=>make(Array.from({length:129},(_,i)=>`synthetic:${i}`)));
});
test("nodes cannot call other nodes or run after cancellation", async () => {
 const n = defineNode<Record<string,never>,{totalCents:string},null>({name:"fixture/nested",version:"1.0.0",description:"forbidden nested call",input:{type:"object"},output:quote.descriptor.outputSchema,dependencies:null,execute:async(c)=>quote.invoke(c,{sku:"coffee",quantity:2})});
 await assert.rejects(n.invoke(ctx(),{}),/Node operation failed/);
 const controller = new AbortController();controller.abort();await assert.rejects(quote.invoke({...ctx(),signal:controller.signal},{sku:"coffee",quantity:2}));
});
test("descriptor defaults are deeply immutable and invalid outputs are node failures",async()=>{
 const schema=compileSchema({type:"object",properties:{x:{type:"array",items:{type:"string"},default:["safe"]}}});
 assert.throws(()=>{(schema.properties!.x!.default as string[]).push("mutated");});
 assert.equal(normalizeJSON(schema,"{}"),'{"x":["safe"]}');
 const invalid=defineNode<Record<string,never>,{value:string},null>({name:"fixture/invalid",version:"1.0.0",description:"Invalid output fixture",input:{type:"object"},output:{type:"object",properties:{value:{type:"integer",wire:"int64-string"}},required:["value"]},dependencies:null,execute:()=>({value:"overflow"})});
 await assert.rejects(invalid.invoke(ctx(),{}),(error:unknown)=>error instanceof DomainError&&error.classification==="NODE_ERROR"&&error.code==="invalid_output");
});
