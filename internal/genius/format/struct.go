package format

import (
	"google.golang.org/protobuf/types/known/structpb"
)

// StructToStringMap converts *structpb.Struct to map[string]interface{}
func StructToStringMap(v *structpb.Struct) map[string]interface{} {
	return v.AsMap()
}

// StringMapToStruct converts map[string]interface{} to *structpb.Struct
func StringMapToStruct(v map[string]interface{}) (*structpb.Struct, error) {
	return structpb.NewStruct(v)
}
