package gcp

import (
	"flag"
)

var (
	Zone    = flag.String("gcp-zone", "", "GCP zone")
	Project = flag.String("gcp-project-id", "", "GCP project ID")
	Network = flag.String("gcp-network", "", "GCP VPC network")
)
