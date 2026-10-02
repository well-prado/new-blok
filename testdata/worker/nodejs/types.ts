import { defineNode } from "../../../sdk/nodejs/index.js";
defineNode<{quantity:number},{total:number},null>({name:"fixture/types",version:"1.0.0",description:"typed fixture",input:{type:"object"},output:{type:"object"},dependencies:null,
 // @ts-expect-error An incompatible output cannot implement this typed node.
 execute:()=>({total:"not-a-number"}) });
