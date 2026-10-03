import { createHash } from "node:crypto";

export interface BlobReference { readonly digest:string;readonly size:number }
// Inject the reader as a dependency. Neither payload nor model content selects
// endpoint, credential or principal. This is not a native-code sandbox.
export function createBlobReader(endpoint:string,token:string,principal:string,maxBytes=8<<20){
 const url=new URL(endpoint);
 if(url.protocol!=="http:" || url.hostname!=="127.0.0.1" || !url.port || url.pathname!=="/" || url.search || url.hash || url.username || url.password || Buffer.byteLength(token)<32 || Buffer.byteLength(token)>4096 || !/^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/.test(principal) || !Number.isSafeInteger(maxBytes) || maxBytes<1 || maxBytes>8<<20)throw new Error("invalid_blob_configuration");
 return async(ref:BlobReference,signal:AbortSignal):Promise<Uint8Array>=>{
  if(!/^sha256:[0-9a-f]{64}$/.test(ref.digest) || !Number.isSafeInteger(ref.size) || ref.size<0 || ref.size>maxBytes)throw new Error("invalid_blob_reference");
  signal.throwIfAborted();
  try{
   const response=await fetch(new URL(`blobs/${ref.digest}?size=${ref.size}`,url),{headers:{authorization:`Bearer ${token}`,"x-blok-principal":principal},redirect:"error",signal});
   if(!response.ok || response.headers.get("content-length")!==String(ref.size))throw new Error("blob_denied");
   const reader=response.body?.getReader();if(!reader)throw new Error("blob_unavailable");let size=0;const chunks:Uint8Array[]=[];
   while(true){const part=await reader.read();if(part.done)break;size+=part.value.byteLength;if(size>ref.size){await reader.cancel();throw new Error("blob_limit");}chunks.push(part.value);}
   const data=Buffer.concat(chunks);if(data.byteLength!==ref.size || `sha256:${createHash("sha256").update(data).digest("hex")}`!==ref.digest)throw new Error("blob_digest");signal.throwIfAborted();return data;
  }catch{throw new Error("blob_read_failed");}
 };
}
