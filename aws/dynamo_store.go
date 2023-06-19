package aws

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/akmistry/cloud-util"
)

const (
	scheme = "aws"

	queryParamRegion = "r"
)

type DynamoStore struct {
	client    *dynamodb.Client
	tableName string
}

var _ = (cloud.UnorderedStore)((*DynamoStore)(nil))

func init() {
	cloud.RegisterStoreScheme(scheme, openStore)
}

// URL format: aws://dynamodb/<table name>[?r=<region>]
func openStore(path string) (cloud.UnorderedStore, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	if u.Scheme != scheme {
		return nil, fmt.Errorf("cloud/aws: unexpected scheme: %s", u.Scheme)
	} else if u.Host == "" {
		return nil, fmt.Errorf("cloud/aws: empty type")
	} else if u.Path[0] != '/' {
		return nil, fmt.Errorf("cloud/aws: invalid path: %s", u.Path)
	}
	name := u.Path[1:]
	if name == "" {
		return nil, fmt.Errorf("cloud/aws: empty table name")
	}
	region := u.Query().Get(queryParamRegion)

	switch u.Host {
	case "dynamodb":
		var fns []func(*config.LoadOptions) error
		if region != "" {
			fns = append(fns, config.WithRegion(region))
		}
		return NewDefaultDynamoStore(name, fns...)
	default:
		return nil, fmt.Errorf("cloud/aws: invalid type: %s", u.Host)
	}
}

func NewDefaultDynamoStore(tableName string, optFns ...func(*config.LoadOptions) error) (*DynamoStore, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), optFns...)
	if err != nil {
		return nil, err
	}
	return NewDynamoStore(cfg, tableName)
}

func NewDynamoStore(cfg aws.Config, tableName string) (*DynamoStore, error) {
	c := dynamodb.NewFromConfig(cfg)
	s := &DynamoStore{
		client:    c,
		tableName: tableName,
	}
	return s, nil
}

func (s *DynamoStore) makeKey(key string) map[string]types.AttributeValue {
	v, err := attributevalue.Marshal(key)
	if err != nil {
		panic(err)
	}
	return map[string]types.AttributeValue{
		"key": v,
	}
}

func (s *DynamoStore) Get(key string) (*cloud.KVPair, error) {
	out, err := s.client.GetItem(context.TODO(),
		&dynamodb.GetItemInput{
			TableName:      aws.String(s.tableName),
			ConsistentRead: aws.Bool(true),
			Key:            s.makeKey(key),
		})
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return nil, cloud.ErrKeyNotFound
		}
		return nil, err
	} else if out.Item == nil {
		return nil, cloud.ErrKeyNotFound
	}
	valAttr := out.Item["value"]
	if val, ok := valAttr.(*types.AttributeValueMemberB); ok {
		return &cloud.KVPair{Key: key, Value: val.Value}, nil
	}
	return nil, fmt.Errorf("Unable to get value")
}

func (s *DynamoStore) Exists(key string) (bool, error) {
	// TODO: Use an actual "exists" method.
	_, err := s.Get(key)
	if err == cloud.ErrKeyNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (s *DynamoStore) Put(key string, value []byte, options *cloud.WriteOptions) error {
	item := map[string]interface{}{
		"key":   key,
		"value": value,
	}
	itemAttr, err := attributevalue.MarshalMap(item)
	if err != nil {
		panic(err)
	}
	_, err = s.client.PutItem(context.TODO(),
		&dynamodb.PutItemInput{
			TableName: aws.String(s.tableName),
			Item:      itemAttr,
		})
	if err != nil {
		return err
	}
	return nil
}

func (s *DynamoStore) Delete(key string) error {
	panic("Unimplemented")
	return nil
}
