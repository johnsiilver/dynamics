package method

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func BenchmarkStandardCall(b *testing.B){
	ctx := context.Background()
	bl := blah{data: &strings.Builder{}}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		bl.PrintHello(ctx)
		bl.PrintWorld(ctx)
	}
}

func BenchmarkDynamicCall(b *testing.B) {
	ctx := context.Background()
	bl := blah{data: &strings.Builder{}}
	var sigV sig

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for method := range MatchesSignature(reflect.ValueOf(bl), reflect.ValueOf(sigV)) {
			Call(method, ctx)
		}
	}
}
