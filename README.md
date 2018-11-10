# Depot

## Overview

Depot is a persistent queue library. You can store items on disk and later retrieve them as an ordered stream. An item is a collection of bytes (`u8`) and is assigned a monotonically increasing id. The ids are not necessarily sequential.

It's important to note that Depot is focused strictly on low-level storage. Replication and remote access are outside the scope of Depot.

## Goals & Thoughts

* Store data in plain files on disk with tight control over when data is flushed.
* Tolerate crashes and power less, but do so in a lazy fashion.
* Single-threaded writer.
* Support the ability to rewrite sections of the queue in an atomic fashion. This is useful for implementing concepts like compaction.
* Ability to use from multiple languages, notably Rust and JVM.
* Synchronous file I/O. Given the poor state of AIO on Linux, defer to higher level abstractions to emulate asynchronous behavior. For instance, a dedicated group of threads can be used to interact with Depot.
* Network support is left to higher level libraries.
* Potentially support data integrity measures. CRC for each item is being considered, at the cost of 4 bytes of additional fixed overhead per stored item.

## FAQ

### How does Depot store data? What's the overhead?

Depot stores its data in plain files using a binary encoding. An escape mechanism handles collisions on the record separator and failure bytes.

Each record stored in Depot costs a constant byte of overhead, plus ~2% overhead for the encoding mechanism. In the worst case, an item may require 100% of its size to store, if all of its bytes consist of those that need to be escaped. In general, this may increase by four bytes per item if a CRC mechanism is added to the implementation. Additionally, truncated items, which can occur due to power loss or crash, result in two bytes being added to them during recovery.

### How does Depot deal with crashes while writing data?

When opening the queue for appending, it reads the last byte of the file. If it's a *10*, the presumption is that the system hasn't crashed.

However, if it isn't a *10* (and the file is not empty), Depot assumes that the previous writer has crashed, and it appends two *45* values, followed by *10*. The API allows readers to differentiate between items that were fully written and those that were potentially only partially written. Note that it is not possible for these values to occur in an item's encoded payload, as they are translated to other values via an escape/control byte mechanism.

### How fast is Depot?

The low level primitive, *Section*, is largely limited by disk I/O speed. For a very flawed initial test, given a Lenovo Thinkpad, i7-6600U, with a consumer-grade SSD, 50 byte payloads, about 4.7M reads/sec can be performed by a single reader. This translates to ~300MB/sec. For a writer, given the same constraints, about 1.7M writes/sec, translating to ~110MB/sec. Be sure to take these measurements with a grain of salt.

The primary interface, *Queue*, has similar performance characteristics but measurements haven't been done yet.

Given its append only design, it should also perform well with "spinning rust" disks.

### Does it support multiple concurrent writers?

Multiple concurrent writers are not supported. A library such as [semalock](https://github.com/longshorej/semalock) can be used if coordination between processes is required, but it's better to use messaging and a single writer if possible.

### Does Depot support removing records?

Conceptually, yes, but this hasn't been implemented yet. Given that queue's are split into files that contain a bounded number of items, each of these files can be rewritten and then atomically renamed over the old section. On Linux, readers that may have the old file open will continue to work until they release their file descriptor.

### How much data can be stored in Depot?

A single Depot queue can technically store ~3.8PB of data, given a limit of 1.9B files at ~2GB each. This is because depot uses a 64bit offset for efficiently resuming from a position in the queue. 32bits are used to address the file, and 32bits to address the position in the file. You're likely to run into underlying storage limitations before this, whether that is hardware (disk size) or software (filesystem). Nothing close to this has been tested, though.

### Java and Scala?

Support is planned via JNI, see the [jvm](jvm) directory which is working toward a proof of concept to use Depot from Java and Scala code.

## License

Depot is licensed under the Apache License, Version 2. See [LICENSE](LICENSE).

## Author

Jason Longshore <hello@jasonlongshore.com>
