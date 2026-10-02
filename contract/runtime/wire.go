package runtime

import (
 "time"
 "github.com/well-prado/new-blok/contract/runtime/wire"
)

func HelloWire(h Hello) *wire.Hello {
 caps:=make([]string,len(h.Capabilities));for i,c:=range h.Capabilities{caps[i]=string(c)}
 return &wire.Hello{Protocol:h.Protocol,Major:uint32(h.Major),Minor:uint32(h.Minor),ArtifactDigest:h.ArtifactDigest,CatalogDigest:h.CatalogDigest,Generation:h.Generation,Capabilities:caps,Limits:&wire.Limits{MaxFrameBytes:uint32(h.Limits.MaxFrameBytes),MaxBlobBytes:uint32(h.Limits.MaxBlobBytes),MaxConcurrentCalls:uint32(h.Limits.MaxConcurrentCalls)}}
}
func HelloFromWire(h *wire.Hello) Hello {
 if h==nil{return Hello{}};caps:=make([]Capability,len(h.Capabilities));for i,c:=range h.Capabilities{caps[i]=Capability(c)}
 out:=Hello{Protocol:h.Protocol,Major:int(h.Major),Minor:int(h.Minor),ArtifactDigest:h.ArtifactDigest,CatalogDigest:h.CatalogDigest,Generation:h.Generation,Capabilities:caps};if h.Limits!=nil{out.Limits=Limits{int(h.Limits.MaxFrameBytes),int(h.Limits.MaxBlobBytes),int(h.Limits.MaxConcurrentCalls)}};return out
}
func CallWire(c Call)*wire.Call{caps:=make([]string,len(c.Capabilities));for i,v:=range c.Capabilities{caps[i]=string(v)};out:=&wire.Call{CallId:c.CallID,AttemptId:c.AttemptID,Generation:c.Generation,Node:c.Node,NodeVersion:c.NodeVersion,IdempotencyKey:c.IdempotencyKey,DeadlineUnixNanos:c.Deadline.UnixNano(),Input:append([]byte(nil),c.Input...),Principal:c.Principal,Capabilities:caps};for _,b:=range c.Blobs{out.Blobs=append(out.Blobs,&wire.BlobRef{Digest:b.Digest,Size:uint64(b.Size)})};return out}
func CallFromWire(c *wire.Call)Call{if c==nil{return Call{}};out:=Call{CallID:c.CallId,AttemptID:c.AttemptId,Generation:c.Generation,Node:c.Node,NodeVersion:c.NodeVersion,IdempotencyKey:c.IdempotencyKey,Deadline:time.Unix(0,c.DeadlineUnixNanos),Input:append([]byte(nil),c.Input...),Principal:c.Principal};for _,v:=range c.Capabilities{out.Capabilities=append(out.Capabilities,Capability(v))};for _,b:=range c.Blobs{if b!=nil{out.Blobs=append(out.Blobs,BlobRef{b.Digest,int(b.Size)})}};return out}
func ResultFromWire(r *wire.Result)Result{if r==nil{return Result{}};out:=Result{CallID:r.CallId,AttemptID:r.AttemptId,Generation:r.Generation,Output:append([]byte(nil),r.Output...)};if r.Error!=nil{classes:=map[wire.ErrorClass]ErrorClass{wire.ErrorClass_INVALID_INPUT:ErrorInvalidInput,wire.ErrorClass_NODE_ERROR:ErrorNode,wire.ErrorClass_TRANSIENT:ErrorTransient,wire.ErrorClass_UNCERTAIN:ErrorUncertain,wire.ErrorClass_CANCELED:ErrorCanceled,wire.ErrorClass_DEADLINE_EXCEEDED:ErrorDeadline};class,ok:=classes[r.Error.Class];if !ok{class=ErrorNode};out.Error=&RemoteError{Class:class,Code:r.Error.Code,Message:r.Error.Message,Retryable:r.Error.Retryable,IdempotencyKey:r.Error.IdempotencyKey}};return out}
