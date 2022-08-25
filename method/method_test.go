package method

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type sig func(ctx context.Context) string

type blah struct {
	data *strings.Builder
}

func (b blah) PrintHello(ctx context.Context) string {
	b.data.WriteString("hello")
	return "hello"
}

func (b blah) PrintWorld(ctx context.Context) string {
	b.data.WriteString("world")
	return "world"
}

func (b blah) privateSig(ctx context.Context) string {
	b.data.WriteString("private")
	return "private"
}

func (b blah) PrintThisIsBad() string {
	b.data.WriteString("bad")
	return "bad"
}

func (b blah) PrintBad2(what string) string {
	b.data.WriteString("bad2")
	return "bad2" + what
}

type privatesWithInterface interface {
	PrintHello(ctx context.Context) string
	PrintWorld(ctx context.Context) string
	privateSig(ctx context.Context) string
}

func TestMatchesSignature(t *testing.T) {
	var sigV sig
	b := blah{data: &strings.Builder{}}

	ctx := context.Background()

	for method := range MatchesSignature(reflect.ValueOf(b), reflect.ValueOf(sigV)) {
		returnVals := Call(method, ctx)
		fmt.Println(returnVals[0].String())
	}

	if b.data.String() != "helloworld" {
		t.Fatalf("TestMatchesSignature(object): got %q, want %q", b.data.String(), "helloworld")
	}
}
