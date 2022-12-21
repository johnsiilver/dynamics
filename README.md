# dynamics
Go dynamic operations, usually through reflection.

## Introduction

This module contains packages that do various dynamic operations via reflection or similar methods.

## Method Package

The `method` package provides `MatchesSignature()` for dynamically discoverying methods on an object that matches a signature. You can then use the `Call()` function to call the methods.

This allows you to define methods on an object that match a signature and can be discovered and called dynamically.

It should be noted that calling a method this way is significantly slower:

```
BenchmarkStandardCall-10        24158559                70.37 ns/op
BenchmarkDynamicCall-10           408691              2970 ns/op
```

However, nanosecond to microssecond for most functions is not going to matter. But if the function is in a tight loop and is performance critical, don't use this.

## Demux Package 

The `demux` package provides a generic demuxer.  It take messages with some
type of routing and routes them to channels that match those IDs. It has custom error handling, middleware support and other features.

The Demux type can process requests that were multiplexed onto a single stream and demux them into separate channels. 

The InOrder type can be used to take Demux messages from an output channel and order them. This is as long as the message contains an integer type that sends messages in ascending order starting from 0. Cannot contain duplicates.