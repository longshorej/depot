# Depot

## Overview

Depot is a persistent queue library. You can store items on disk and later retrieve them as an ordered stream. An item is a collection of bytes (`u8`) and is assigned a monotonically increasing id.

Each item additionally has an associated offset which can be used when retrieving items to resume from that position in the queue. Additionally, you can choose to start reading from the end of the queue if that is desired.

It's important to note that Depot is focused strictly on low-level storage. Replication and remote access are left to higher level libraries.

## Goals & Thoughts

* Per usual, the goal here is simplicity.
* Store data in plain files on disk with tight control over when data is flushed.
* Tolerate crashes, but do so in a lazy fashion.
* Single-threaded writer.
* Support the ability to rewrite sections of the queue in an atomic fashion. This is useful for implementing concepts like compaction.
* Ability to use from multiple languages, notably Rust and JVM.
* Synchronous file I/O. Given the poor state of AIO on Linux, defer to higher level abstractions to emulate asynchronous behavior. For instance, a dedicated group of threads can be used to interact with Depot.
* Network support is left to higher level libraries.
* Topics, or partitioning of items, may be supported natively in Depot by composing its underlying components. TBD.
* Potentially support data integrity measures. CRC for each item is being considered, at the cost of 4 bytes of additional fixed overhead per stored item.

## FAQ

### How does Depot store data? What's the overhead?

Depot stores its data in plain files, and uses a binary encoding to store its data. An escape mechanism handles collisions on the record separator and failure bytes.

Each record stored in Depot costs a constant 5 bytes of overhead, plus ~2% overhead for the encoding mechanism.

### How does Depot deal with crashes while writing data?

When opening the queue for appending, it reads the last character of the file. If it's a newline, the presumption is that the system hasn't crashed.

However, if it isn't a newline (and the file is not empty), Depot assumes that the previous writer has crashed, and it appends a *-* character, 0xTBD, followed by a newline, *\n*. The API allows readers to differentiate between items that were fully written and those that were potentially only partially written. Note that if the system crashes during crash recovery, the corrupted items may accumulate *-* characters.

### How fast is Depot?

The low level primitive, *QueueSection*, is largely limited by disk I/O speed. For a very flawed initial test, given a Lenovo Thinkpad, i7-6600U, with a consumer-grade SSD, 50 byte payloads, about 4.7M reads/sec can be performed by a single reader. This translates to ~300MB/sec. For a writer, given the same constraints, about 1.7M writes/sec, translating to ~110MB/sec. Be sure to take it with a grain of salt.

The higher level interface hasn't been written yet, but it shouldn't incur much additional overhead other than switching between files.

Given its append only design, it should also perform well with "spinning rust" disks.

### Does it support multiple concurrent writers?

Multiple concurrent writers are not supported. A library such as [semalock](https://github.com/longshorej/semalock) can be used if coordination between processes is required. It's better to use messaging and a single writer if possible though.

### Does Depot support removing records?

Conceptually, yes, but this hasn't been implemented yet. Given that queue's are split into files that contain a bounded number of items, each of these files can be rewritten and then atomically renamed over the old section.

### Java and Scala?

Support is planned via JNI, see the [jvm](jvm) which is working toward a proof of concept to use Depot from Java and Scala code.

## Impl Notes

* A *QueueSection* can have upto 1,048,576 items stored in it, which means ~1GB files if each item is 1KB. An item can be upto 64KB in length, meaning the maximum size for a given section is 64GB. This is important to keep removal / rewriting workable, as entire sections need to be rewritten.
* 36 bits are used to store file offsets, and 28 bits are used to store the section number. This can be encoded as 8 hex characters, with a maximum value of 10000000 (hex).
* A *Queue* stores its data in *QueueSection*s which are themselves assigned ids. Once a queue is full, a new one is created with the next id. IDs are encoded as hex strings that are zero-padded to 6 digits. This is grouped into two 2 digit groups, the parent directories, and a 3 digit hex encoding as the final name. For instance, given ABCDEF12, it would be stored in `<storage-dir>/ab/cd/ef12`.
* This means that there can a total of 268,435,456 files on the system, each containing 1,048,576 items, for a total of 17,592,186,044,416 items. If creating 1M items per second, this would be exh
* Ammendment: probably will change most of the above


## License

Depot is licensed under the Apache License, Version 2. See [LICENSE](LICENSE).

## Author

Jason Longshore <hello@jasonlongshore.com>
