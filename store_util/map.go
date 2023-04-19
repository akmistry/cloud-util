package store_util

import (
	"strings"
	"sync"

	"github.com/akmistry/cloud-util"
)

func MapKeys(s cloud.OrderedStore, startKey, endKey string, f func(key string), workers int) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	var keysCh chan []string
	if workers > 0 {
		wg.Add(workers)
		keysCh = make(chan []string)
		defer close(keysCh)

		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				for kl := range keysCh {
					for _, k := range kl {
						f(k)
					}
				}
			}()
		}
	}

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
		if endKey != "" {
			for i, k := range keys {
				if strings.Compare(k, endKey) >= 0 {
					keys = keys[:i]
					endLoop = true
					break
				}
			}
		}
		if len(keys) == 0 {
			break
		}

		if keysCh != nil {
			keysCh <- keys
		} else {
			for _, k := range keys {
				f(k)
			}
		}
		startKey = keys[len(keys)-1]
		skipStartKey = true
	}

	return nil
}
