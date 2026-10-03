import { defineNode } from "../../runtime/nodejs/dist/sdk/nodejs/index.js";
// Synthetic composition root: trusted Go test injects one loopback provider.
const endpoint = process.env.BLOK_SYNTHETIC_EFFECT_URL;
if (!endpoint || !/^http:\/\/127\.0\.0\.1:[0-9]+$/.test(endpoint)) throw new Error("synthetic provider required");
export const nodes = [defineNode({
  name: "fixture/charge", version: "1.0.0", description: "Synthetic authority-bound HTTP charge",
  input: {type:"object",properties:{},additionalProperties:false},
  output: {type:"object",properties:{done:{type:"boolean"}},required:["done"]},
  effects:["http:charges"],requiredCapabilities:["http:charges"],dependencies:{endpoint},
  async execute(ctx,_input,deps){const response=await fetch(deps.endpoint,{method:"POST",signal:ctx.signal});return response.json();},
})];
