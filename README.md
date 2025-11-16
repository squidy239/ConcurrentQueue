# ConcurrentQueue

**A thread-safe, multi-producer multi-consumer queue written in Zig that uses fragment-based sharding to minimize lock contention.**

## Overview

ConcurrentQueue is a high-performance concurrent queue implementation in Zig. It is designed for scenarios where multiple threads need to safely enqueue and dequeue items with minimal lock contention. The queue leverages **fragment-based sharding**, allowing multiple producers and consumers to interact with different queue fragments simultaneously, reducing bottlenecks and improving overall throughput.

## Features

- **Thread-safe:** Supports multiple producers and multiple consumers concurrently.
- **Fragment-based sharding:** Minimizes lock contention by partitioning the queue.
- **Pure Zig implementation:** No external dependencies.
- **High performance:** Designed with efficiency and contention reduction in mind.

## Usage

### Installation

Clone the repository into your Zig project or include it as a Git submodule:

```sh
zig fetch --save git+https://github.com/squidy239/ConcurrentQueue.git
```

Then, in your `build.zig`:

```zig
    const ConcurrentQueue = b.dependency("ConcurrentQueue", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("ConcurrentQueue", ConcurrentQueue.module("ConcurrentQueue"));
```

### Example

```zig

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    var queue = ConcurrentQueue(i32, 16, true).init(allocator);
    defer queue.deinit(false);
    _ = try queue.append(42);
    if (queue.popFirst()) |value| {
        std.debug.print("Dequeued value: {}\n", .{value});
    }
}
```

### API Reference

ConcurrentQueue(DataType, fragments, strictFIFO)

- DataType - Type to store
- fragments - Number of internal shards
- strictFIFO - true for perfect ordering ordered, false for faster speed

Methods
- ziginit(allocator)           // Create queue
- deinit(assertEmpty)       // Cleanup (assertEmpty: panic if not empty)
- append(data)              // Add item → !removable
- note: removable is currently not yet implemented
- popFirst()                // Remove first → ?DataType
