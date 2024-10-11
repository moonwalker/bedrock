package streams

import (
	"time"

	"github.com/nats-io/nkeys"
)

func NKeySignatureHandler(seed string, b []byte) ([]byte, error) {
	sk, err := nkeys.FromSeed([]byte(seed))
	if err != nil {
		return nil, err
	}
	return sk.Sign(b)
}

func GetHeader(headers map[string][]string, name string) string {
	for hn, hv := range headers {
		if hn == name {
			if len(hv) > 0 {
				return hv[0]
			}
		}
	}
	return ""
}

func getElapsed(start time.Time) string {
	return time.Since(start).String()
}
