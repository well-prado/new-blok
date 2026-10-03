import { readFileSync } from "node:fs";
const report=JSON.parse(readFileSync(process.argv[2],"utf8"));
if(report.SchemaVersion!==2 || !/^[0-9a-f]{40}$/.test(report.SourceRevision) || !report.StartedAtUTC || !report.FinishedAtUTC || report.StartupSamples.length!==5 || report.FaultSamples.length!==10)throw new Error("missing repeated evidence or provenance");
for(const mode of ["native","node"]){
 const batches=report.Samples.filter(s=>s.mode===mode);
 if(batches.length!==5 || batches.some((s,i)=>s.nanoseconds.length!==50 || s.Requests!==20+(i+1)*50 || s.Effects!==s.Requests || !(s.GoHarnessRSSSnapshotKiB>0) || (mode==="node" && !(s.WorkerRSSSnapshotKiB>0))))throw new Error("unequal batches, effects or missing memory snapshots");
 const samples=batches.flatMap(s=>s.nanoseconds).sort((a,b)=>a-b);
 if(samples.length!==250)throw new Error("unexpected evidence sample count");
 if(samples.some(n=>!Number.isSafeInteger(n) || n<=0))throw new Error("invalid latency sample");
 const percentile=p=>samples[Math.floor(samples.length*p)]/1e6;
 process.stdout.write(JSON.stringify({mode,samples:samples.length,p50ms:percentile(.5),p95ms:percentile(.95),p99ms:percentile(.99),serialOrdersPerSecond:samples.length*1e9/samples.reduce((a,b)=>a+b,0)})+"\n");
}
if(report.FaultSamples.some(s=>s.Requests!==1 || s.Effects!==(s.Phase==="late"?1:0) || !["delay","late"].includes(s.Phase) || s.ErrorClass!=="uncertain" || s.PublishedReceipts!==0))throw new Error("unexpected fault outcome/effects");
if(["delay","late"].some(phase=>report.FaultSamples.filter(s=>s.Phase===phase).length!==5))throw new Error("missing repeated crash phase");
process.stdout.write(JSON.stringify({startupms:report.StartupSamples.map(s=>s.StartupNanoseconds/1e6),workerRssReadySnapshotKiB:report.StartupSamples.map(s=>s.WorkerRSSSnapshotKiB),goHarnessRssReadySnapshotKiB:report.StartupSamples.map(s=>s.GoHarnessRSSSnapshotKiB),faultSamples:report.FaultSamples.length,sourceRevision:report.SourceRevision,startedAtUTC:report.StartedAtUTC,finishedAtUTC:report.FinishedAtUTC,go:report.Go,node:report.Node,os:report.OS,arch:report.Arch})+"\n");
