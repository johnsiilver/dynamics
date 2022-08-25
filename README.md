# dynamics
Go dynamic operations through reflection

## Introduction

This module contains packages that do various dynamic operations via reflection.

## Method Package

The `method` package provides `MatchesSignature()` for dynamically discoverying methods on an object that matches a signature. You can then use the `Call()` function to call the methods.

This allows you to define methods on an object that match a signature and can be discovered and called dynamically.

It should be noted that calling a method this way is significantly slower:

```
BenchmarkStandardCall-10        24158559                70.37 ns/op
BenchmarkDynamicCall-10           408691              2970 ns/op
```

However, nanosecond to microssecond for most functions is not going to matter. But if the function is in a tight loop and is performance critical, don't use this.
