package main

import (
	"strings"
	"testing"
)

var target1 = strings.ToLower("17F165D5A5BA695F27C023A83AA2B3463E23810E360B7517127E90161EEBABDA")

func BenchmarkCalcHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		calcHash(target1, "", int64(0), int64(186))
	}
}

func TestCalcHash(t *testing.T) {
	if calcHash(target1, "", int64(0), int64(185))
}
