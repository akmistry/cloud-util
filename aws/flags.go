package aws

import (
	"flag"
)

var (
	Endpoint     = flag.String("aws-endpoint", "localhost:9000", "AWS endpoint URL")
	AccessId     = flag.String("aws-access-id", "FJGGfFQWdSqfywBu", "AWS access ID")
	AccessSecret = flag.String("aws-access-secret", "A81e1gVP3cNw6rjsDE9ifU7IWLWYHB42",
		"AWS access secret")
	Region = flag.String("aws-region", "us-east-1", "AWS region")
)
