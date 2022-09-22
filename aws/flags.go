package aws

import (
	"flag"
)

var (
	Endpoint     = flag.String("aws-endpoint", "localhost:9000", "AWS endpoint URL")
	AccessId     = flag.String("aws-access-id", "JQWCMX7KVXJ6AA1DY1WW", "AWS access ID")
	AccessSecret = flag.String("aws-access-secret", "/n6/bYBOsYoY4lfBmtxlh8qroSwcXoJc+Fn/mRUd",
		"AWS access secret")
	Region = flag.String("aws-region", "us-east-1", "AWS region")
)
