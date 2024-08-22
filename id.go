package queuelite

import "math/rand"

// newRandomID returns a 20-character long pseudo-random string made of letters and numbers.
//
// see https://stackoverflow.com/a/31832326
func newRandomID() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	const size = 20
	b := make([]byte, size)
	for i := range b {
		b[i] = chars[rand.Int63()%int64(len(chars))]
	}
	return string(b)
}
