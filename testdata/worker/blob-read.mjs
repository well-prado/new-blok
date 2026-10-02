// Synthetic Node client for actual authenticated blob socket conformance.
import { pathToFileURL } from "node:url";
const { createBlobReader }=await import(pathToFileURL(process.env.BLOK_BLOB_SDK).href);
try{
 const reader=createBlobReader(process.env.BLOK_BLOB_ENDPOINT,process.env.BLOK_BLOB_TOKEN,process.env.BLOK_BLOB_PRINCIPAL);
 const data=await reader({digest:process.env.BLOK_BLOB_DIGEST,size:Number(process.env.BLOK_BLOB_SIZE)},AbortSignal.timeout(2000));
 process.stdout.write(`${data.length}\n`);
}catch{process.stderr.write("blob_read_failed\n");process.exitCode=1;}
