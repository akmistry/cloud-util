package util

import (
	"google.golang.org/protobuf/proto"

	"github.com/akmistry/cloud-util"
)

func GetProto(s cloud.UnorderedStore, key string, msg proto.Message) error {
	val, err := s.Get(key)
	if err != nil {
		return err
	}

	return proto.Unmarshal(val.Value, msg)
}

func PutProto(s cloud.UnorderedStore, key string, msg proto.Message) error {
	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.Put(key, buf, nil)
}
