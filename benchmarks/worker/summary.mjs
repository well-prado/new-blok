import { readFileSync } from "node:fs";
const report=JSON.parse(readFileSync(process.argv[2],"utf8"));
for(const mode of ["native","node"]){
 const samples=report.Samples.filter(s=>s.mode===mode).flatMap(s=>s.nanoseconds).sort((a,b)=>a-b);
 if(samples.length!==250)throw new Error("unexpected evidence sample count");
 const percentile=p=>samples[Math.floor(samples.length*p)]/1e6;
 process.stdout.write(JSON.stringify({mode,samples:samples.length,p50ms:percentile(.5),p95ms:percentile(.95),p99ms:percentile(.99),serialOrdersPerSecond:samples.length*1e9/samples.reduce((a,b)=>a+b,0)})+"\n");
}
process.stdout.write(JSON.stringify({startupms:report.StartupNanoseconds/1e6,workerRssSnapshotKiB:report.WorkerRSSKiB,go:report.Go,node:report.Node,os:report.OS,arch:report.Arch})+"\n");
