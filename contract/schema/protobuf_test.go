package schema

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestNativeJSONAndProtobufCorpusOutputsMatch(t *testing.T) {
	s, err := Parse([]byte(`{"type":"object","properties":{"total":{"type":"integer","wire":"int64-string"}},"required":["total"],"additionalProperties":false}`))
	if err != nil {
		t.Fatal(err)
	}
	native, err := s.NormalizeValue(map[string]any{"total": int64(9223372036854775807)})
	if err != nil {
		t.Fatal(err)
	}
	nativeJSON, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: protoString("schema.proto"), Syntax: protoString("proto3"), Package: protoString("corpus"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: protoString("Value"), Field: []*descriptorpb.FieldDescriptorProto{{Name: protoString("total"), JsonName: protoString("total"), Number: protoInt32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := file.Messages().ByName("Value")
	message := dynamicpb.NewMessage(descriptor)
	field := descriptor.Fields().ByName("total")
	message.Set(field, protoreflect.ValueOfString("9223372036854775807"))
	protobufJSON, err := (protojson.MarshalOptions{}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := s.Normalize([]byte(`{"total":"9223372036854775807"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(nativeJSON) != string(wire) || string(protobufJSON) != string(wire) {
		t.Fatalf("native=%s protobuf=%s wire=%s", nativeJSON, protobufJSON, wire)
	}
}

func protoString(v string) *string { return &v }
func protoInt32(v int32) *int32    { return &v }
