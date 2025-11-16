const std = @import("std");

///Creates a thread safe multi producer multi consumer queue that uses fragments to minimise locking.
///If strictFIFO is false it will be close to FIFO but may not be perfectly ordered but it will be a lot faster when many threads are using it
pub fn ConcurrentQueue(comptime DataType: type, comptime fragments: usize, comptime strictFIFO: bool) type {
    return struct {
        setfragmentindex: std.atomic.Value(u64),
        getfragmentindex: std.atomic.Value(u64),
        getfragmentindexlock: std.Thread.RwLock,
        allocators: [fragments]std.mem.Allocator,
        fragments: [fragments]std.DoublyLinkedList,
        fragmentLocks: [fragments]std.Thread.Mutex,
        pub fn init(allocator: std.mem.Allocator) @This() {
            var queue = @This(){
                .setfragmentindex = std.atomic.Value(u64).init(0),
                .getfragmentindex = std.atomic.Value(u64).init(0),
                .getfragmentindexlock = .{},
                .fragments = @splat(.{}),
                .allocators = undefined,
                .fragmentLocks = @splat(.{}),
            };
            queue.allocators = @splat(allocator);
            return queue;
        }

        pub fn deinit(self: *@This(), assertEmpty: bool) void {
            for (&self.fragments, 0..) |*fragment, i| {
                while (fragment.popFirst()) |node| {
                    if (assertEmpty) {
                        unreachable;
                    }
                    const datanode: *Node = @fieldParentPtr("node", node);
                    self.allocators[i].destroy(datanode);
                }
            }
        }

        pub fn append(self: *@This(), data: DataType) !removable {
            const index = @rem(self.setfragmentindex.fetchAdd(1, .seq_cst), fragments);
            self.fragmentLocks[index].lock();
            defer self.fragmentLocks[index].unlock();
            const nodePtr = try self.allocators[index].create(Node);
            nodePtr.* = Node{
                .data = data,
                .node = .{},
            };
            self.fragments[index].append(&nodePtr.node);
            return removable{ .index = index, .node = &nodePtr.node };
        }

        pub const removable = struct {
            index: usize,
            node: *std.DoublyLinkedList.Node,
        };

        pub fn popFirst(self: *@This()) ?DataType {
            var index: usize = undefined;
            if (strictFIFO) self.getfragmentindexlock.lock();

            index = @rem(self.getfragmentindex.fetchAdd(1, .seq_cst), fragments);
            self.fragmentLocks[index].lock();
            defer self.fragmentLocks[index].unlock();
            if (self.fragments[index].popFirst()) |node| {
                if (strictFIFO) self.getfragmentindexlock.unlock();
                const datanode: *Node = @fieldParentPtr("node", node);
                const data: DataType = datanode.data;
                self.allocators[index].destroy(datanode);
                return data;
            } else {
                _ = self.getfragmentindex.fetchSub(1, .seq_cst);
                if (strictFIFO) self.getfragmentindexlock.unlock();
            }
            return null;
        }

        pub const Node = struct {
            data: DataType,
            node: std.DoublyLinkedList.Node,
        };
    };
}

test "queue" {
    var queue = ConcurrentQueue(u32, 32, true).init((std.testing.allocator));
    defer queue.deinit(true);
    _ = try queue.append(12);
    _ = try queue.append(43);
    try std.testing.expectEqual(@as(u32, 12), queue.popFirst());
    try std.testing.expectEqual(@as(u32, 43), queue.popFirst());
    try std.testing.expectEqual(null, queue.popFirst());
    try std.testing.expectEqual(null, queue.popFirst());

    for (0..100) |i| {
        _ = try queue.append(@intCast(i));
    }

    for (0..100) |i| {
        const p = queue.popFirst();
        try std.testing.expectEqual(@as(u32, @intCast(i)), p);
    }
    try std.testing.expectEqual(null, queue.popFirst());
    std.debug.print("singlethreaded pass\n", .{});
}

fn qget(T: type, q: T, i: *u32, imut: *std.Thread.Mutex) void {
    imut.lock();
    defer imut.unlock();
    std.debug.assert(i.* == q.popFirst());
    i.* += 1;
}

test "benchmark: single threaded throughput - strict FIFO" {
    const allocator = std.heap.smp_allocator;
    var queue = ConcurrentQueue(u64, 32, true).init(allocator);
    defer queue.deinit(true);
    const operations = 100_000;
    var timer = try std.time.Timer.start();

    // Benchmark append
    const append_start = timer.lap();
    for (0..operations) |i| {
        _ = try queue.append(i);
    }
    const append_time = timer.lap() - append_start;

    // Benchmark pop
    const pop_start = timer.lap();
    for (0..operations) |_| {
        _ = queue.popFirst();
    }
    const pop_time = timer.lap() - pop_start;

    const append_ns_per_op = append_time / operations;
    const pop_ns_per_op = pop_time / operations;

    std.debug.print("\n[Single Thread - Strict FIFO]\n", .{});
    std.debug.print("  Append: {} ns/op ({d:.2} M ops/s)\n", .{ append_ns_per_op, 1000.0 / @as(f64, @floatFromInt(append_ns_per_op)) });
    std.debug.print("  Pop:    {} ns/op ({d:.2} M ops/s)\n", .{ pop_ns_per_op, 1000.0 / @as(f64, @floatFromInt(pop_ns_per_op)) });
}

test "benchmark: single threaded throughput - non-strict FIFO" {
    const allocator = std.heap.smp_allocator;
    var queue = ConcurrentQueue(u64, 32, false).init(allocator);
    defer queue.deinit(true);
    const operations = 100_000;
    var timer = try std.time.Timer.start();

    const append_start = timer.lap();
    for (0..operations) |i| {
        _ = try queue.append(i);
    }
    const append_time = timer.lap() - append_start;

    const pop_start = timer.lap();
    for (0..operations) |_| {
        _ = queue.popFirst();
    }
    const pop_time = timer.lap() - pop_start;

    const append_ns_per_op = append_time / operations;
    const pop_ns_per_op = pop_time / operations;

    std.debug.print("\n[Single Thread - Non-Strict FIFO]\n", .{});
    std.debug.print("  Append: {} ns/op ({d:.2} M ops/s)\n", .{ append_ns_per_op, 1000.0 / @as(f64, @floatFromInt(append_ns_per_op)) });
    std.debug.print("  Pop:    {} ns/op ({d:.2} M ops/s)\n", .{ pop_ns_per_op, 1000.0 / @as(f64, @floatFromInt(pop_ns_per_op)) });
}

test "benchmark: multithreaded throughput - strict FIFO" {
    const allocator = std.heap.smp_allocator;
    const BenchContext = struct {
        queue: *ConcurrentQueue(u64, 32, true),
        operations: usize,
    };
    const worker = struct {
        fn run(ctx: *BenchContext) void {
            for (0..ctx.operations) |i| {
                _ = ctx.queue.append(i) catch unreachable;
            }
            for (0..ctx.operations) |_| {
                while (ctx.queue.popFirst() == null) {
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run;

    var queue = ConcurrentQueue(u64, 32, true).init(allocator);
    defer queue.deinit(true);

    const num_threads = 16;
    const ops_per_thread = 12_500;
    const total_ops = ops_per_thread * 2; // *2 for append + pop

    var threads: [num_threads]std.Thread = undefined;
    var context = BenchContext{ .queue = &queue, .operations = ops_per_thread };

    var timer = try std.time.Timer.start();
    const start = timer.lap();

    for (0..num_threads) |i| {
        threads[i] = try std.Thread.spawn(.{}, worker, .{&context});
    }

    for (0..num_threads) |i| {
        threads[i].join();
    }

    const elapsed = timer.lap() - start;
    const ns_per_op = elapsed / total_ops;

    std.debug.print("\n[{} Threads - Strict FIFO]\n", .{num_threads});
    std.debug.print("  Total ops: {}\n", .{total_ops});
    std.debug.print("  Time: {d:.2} ms\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000.0});
    std.debug.print("  Throughput: {} ns/op ({d:.2} M ops/s/T)\n", .{ ns_per_op, 1000.0 / @as(f64, @floatFromInt(ns_per_op)) });
}
test "benchmark: multithreaded throughput - non-strict FIFO" {
    const allocator = std.heap.smp_allocator;
    const BenchContext = struct {
        queue: *ConcurrentQueue(u64, 32, false),
        operations: usize,
    };
    const worker = struct {
        fn run(ctx: *BenchContext) void {
            for (0..ctx.operations) |i| {
                _ = ctx.queue.append(i) catch unreachable;
            }
            for (0..ctx.operations) |_| {
                while (ctx.queue.popFirst() == null) {
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run;

    var queue = ConcurrentQueue(u64, 32, false).init(allocator);
    defer queue.deinit(true);

    const num_threads = 16;
    const ops_per_thread = 12_500;
    const total_ops =  ops_per_thread * 2;

    var threads: [num_threads]std.Thread = undefined;
    var context = BenchContext{ .queue = &queue, .operations = ops_per_thread };

    var timer = try std.time.Timer.start();
    const start = timer.lap();

    for (0..num_threads) |i| {
        threads[i] = try std.Thread.spawn(.{}, worker, .{&context});
    }

    for (0..num_threads) |i| {
        threads[i].join();
    }

    const elapsed = timer.lap() - start;
    const ns_per_op = elapsed / total_ops;

    std.debug.print("\n[{} Threads - Non-Strict FIFO]\n", .{num_threads});
    std.debug.print("  Total ops: {}\n", .{total_ops});
    std.debug.print("  Time: {d:.2} ms\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000.0});
    std.debug.print("  Throughput: {} ns/op ({d:.2} M ops/s/T)\n", .{ ns_per_op, 1000.0 / @as(f64, @floatFromInt(ns_per_op)) });
}

test "deinit" {
    var queue = ConcurrentQueue(u32, 8, true).init((std.testing.allocator));
    defer queue.deinit(false);
    _ = try queue.append(12);
    _ = try queue.append(43);
}

test "multithreaded correctness - strict FIFO" {
    const ThreadContext = struct {
        queue: *ConcurrentQueue(u32, 32, true),
        start_value: u32,
        count: u32,
    };
    const producer = struct {
        fn run(ctx: *ThreadContext) void {
            for (0..ctx.count) |i| {
                _ = ctx.queue.append(ctx.start_value + @as(u32, @intCast(i))) catch unreachable;
            }
        }
    }.run;

    const consumer = struct {
        fn run(ctx: *ThreadContext, results: *std.ArrayList(u32), mutex: *std.Thread.Mutex) void {
            for (0..ctx.count) |_| {
                while (true) {
                    if (ctx.queue.popFirst()) |val| {
                        mutex.lock();

                        results.append(std.testing.allocator, val) catch unreachable;
                        mutex.unlock();
                        break;
                    }
                    std.Thread.yield() catch {};
                }
            }
        }
    }.run;

    var queue = ConcurrentQueue(u32, 32, true).init(std.testing.allocator);
    defer queue.deinit(true);

    const num_producers = 100;
    const num_consumers = 100;
    const items_per_producer = 250;

    var producer_threads: [num_producers]std.Thread = undefined;
    var consumer_threads: [num_consumers]std.Thread = undefined;
    var contexts: [num_producers]ThreadContext = undefined;
    var consumer_contexts: [num_consumers]ThreadContext = undefined;

    var results = try std.ArrayList(u32).initCapacity(std.testing.allocator, num_consumers);
    defer results.deinit(std.testing.allocator);
    var results_mutex = std.Thread.Mutex{};

    // Start producers
    for (0..num_producers) |i| {
        contexts[i] = .{
            .queue = &queue,
            .start_value = @intCast(i * items_per_producer),
            .count = items_per_producer,
        };
        producer_threads[i] = try std.Thread.spawn(.{}, producer, .{&contexts[i]});
    }

    // Start consumers
    for (0..num_consumers) |i| {
        consumer_contexts[i] = .{
            .queue = &queue,
            .start_value = 0,
            .count = items_per_producer,
        };
        consumer_threads[i] = try std.Thread.spawn(.{}, consumer, .{ &consumer_contexts[i], &results, &results_mutex });
    }

    // Wait for all threads
    for (0..num_producers) |i| {
        producer_threads[i].join();
    }
    for (0..num_consumers) |i| {
        consumer_threads[i].join();
    }

    // Verify all items received exactly once
    try std.testing.expectEqual(num_producers * items_per_producer, results.items.len);

    var seen = std.AutoHashMap(u32, void).init(std.testing.allocator);
    defer seen.deinit();
    for (results.items) |val| {
        try seen.put(val, {});
    }
    try std.testing.expectEqual(num_producers * items_per_producer, seen.count());
}
