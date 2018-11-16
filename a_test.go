package bdc

import (
	"encoding/binary"
	"testing"
)

func Benchmark_D1(b *testing.B) {
	d := []byte{12, 22, 22, 33}
	for i := 0; i < b.N; i++ {
		binary.BigEndian.Uint32(d)
	}
}

func Benchmark_D2(b *testing.B) {
	f := func(d []byte) uint32 {
		return uint32(d[3]) | uint32(d[2])<<8 | uint32(d[1])<<16 | uint32(d[0])<<24
	}
	d := []byte{12, 22, 22, 33}
	for i := 0; i < b.N; i++ {
		f(d)
	}

}
