package gcp

import (
	"fmt"
	"strings"
)

func RegionFromZone(zone string) string {
	parts := strings.Split(zone, "-")
	if len(parts) != 3 {
		panic(fmt.Sprintf("error parsing zone %s, parts = %v", zone, parts))
	}
	return parts[0] + "-" + parts[1]
}
