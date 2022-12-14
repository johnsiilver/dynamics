// Package method provides
package method

import (
	"fmt"
	"reflect"
)

// MatchesSignature returns all methods on obj that implement sig. sig must be reflect.Kind == refect.Func.
// The values returned are methods that can be called with the .Call() or .CallSlice() method.
// obj and sig reflect.Value of an object and function can be retrieved using reflect.ValueOf(object/function type).
// obj can be an object or interface. Only exported methods will be returned. According to 1.19 docs, if it is
// an interface type, it should return private methods. I have not found this to be true.
//
// Example:
//
//	type sig func(ctx context.Context) string
//
//	type blah struct{}
//	func (b blah) PrintHello(ctx context.Context) string {
//		return "hello"
//	}
//	func (b blah) PrintWorld(ctx context.Context) string {
//		return "world"
//	}
//	func (b blah) PrintThisIsBad() string {
//		return "if you see this, then something is wrong"
//	}
//	func (b blah) PrintBad2(what string) string {
//		return "should see this: " + what
//	}
//
//	func main() {
//		var sigV sig
//		b := blah{}
//
//		ctx := context.Background()
//		ctxVal := reflect.ValueOf(ctx)
//
//		for method := range MatchesSignature(reflect.ValueOf(b), reflect.ValueOf(sigV)) {
//			returnVals := method.Call([]reflect.Value{ctxVal}
//			fmt.Println(returnVals[0].String())
//		}
//
// Caution: you may be tempted to have a method that uses this function to call a set of methods that match
// a signature. You must be careful that the signature of that parent method does not match the signature!
// Otherwise you will enter into a very nasty recursion.
func MatchesSignature(obj reflect.Value, sig reflect.Value) chan reflect.Value {
	if sig.Kind() != reflect.Func {
		panic(fmt.Sprintf("MatchesSignature(): sig must be kind == Func, not %s", sig.Kind()))
	}

	ch := make(chan reflect.Value, 1)

	go func() {
		defer close(ch)
		for i := 0; i < obj.NumMethod(); i++ {
			if obj.Method(i).Type().AssignableTo(sig.Type()) {
				ch <- obj.Method(i)
			}
		}
	}()
	return ch
}

// Call calls a method with args arguments. This makes calling code cleaner. This also accepts reflect.Value.
func Call(method reflect.Value, args ...any) []reflect.Value {
	vArgs := make([]reflect.Value, 0, len(args))
	for _, arg := range args {
		if x, ok := arg.(reflect.Value); ok {
			vArgs = append(vArgs, x)
			continue
		}
		vArgs = append(vArgs, reflect.ValueOf(arg))
	}
	return method.Call(vArgs)
}
