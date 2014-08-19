// Code generated by protoc-gen-go.
// source: metric.proto
// DO NOT EDIT!

/*
Package metric is a generated protocol buffer package.

It is generated from these files:
	metric.proto

It has these top-level messages:
	Point
*/
package metric

import proto "code.google.com/p/goprotobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

type Point struct {
	Path             *string  `protobuf:"bytes,1,req,name=path" json:"path,omitempty"`
	Value            *float64 `protobuf:"fixed64,2,req,name=value" json:"value,omitempty"`
	Timestamp        *uint32  `protobuf:"varint,3,req,name=timestamp" json:"timestamp,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Point) Reset()         { *m = Point{} }
func (m *Point) String() string { return proto.CompactTextString(m) }
func (*Point) ProtoMessage()    {}

func (m *Point) GetPath() string {
	if m != nil && m.Path != nil {
		return *m.Path
	}
	return ""
}

func (m *Point) GetValue() float64 {
	if m != nil && m.Value != nil {
		return *m.Value
	}
	return 0
}

func (m *Point) GetTimestamp() uint32 {
	if m != nil && m.Timestamp != nil {
		return *m.Timestamp
	}
	return 0
}

func init() {
}
