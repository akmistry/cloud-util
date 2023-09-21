package store_util

import (
	"github.com/akmistry/cloud-util"
)

func IterateKeys(s cloud.OrderedStore, startKey string, f func(key string) bool) error {
	skipStartKey := false
	endLoop := false
	for !endLoop {
		keys, err := s.ListKeys(startKey)
		if err != nil {
			return err
		} else if len(keys) == 0 {
			break
		}

		if skipStartKey && keys[0] == startKey {
			keys = keys[1:]
		}
		if len(keys) == 0 {
			break
		}

		for _, k := range keys {
			if !f(k) {
				endLoop = true
				break
			}
		}
		startKey = keys[len(keys)-1]
		skipStartKey = true
	}

	return nil
}
