package cloud

import (
	"flag"
)

var (
	cloudType = flag.String("cloud-type", "local", "Cloud environment. i.e. local, gcp, aws")
)

type CloudType int

const (
	Local CloudType = 1
	Gcp   CloudType = 2
	Aws   CloudType = 3
)

func GetCloudType() CloudType {
	switch *cloudType {
	case "local":
		return Local
	case "gcp":
		return Gcp
	case "aws":
		return Aws
	default:
		panic("invalid --cloud-type")
	}
}
