# Depot

## Overview

Depot is a persistent queue library. You can store items on disk and later retrieve them as an ordered stream. An item is a collection of bytes (`u8`) and is assigned a monotonically increasing id.

Each item additionally has an associated offset which can be used when retrieving items to resume from that position in the queue. Additionally, you can choose to start reading from the end of the queue if that is desired.

It's important to note that Depot is focused strictly on low-level storage. Replication and remote access are left to higher level libraries.

## Goals & Thoughts

* Per usual, the goal here is simplicity.
* Store data in files on disk with tight control over when data is flushed.
* Tolerate crashes, but do so in a lazy fashion.
* Single-threaded writer.
* Support the ability to rewrite sections of the queue in an atomic fashion. This is useful for implementing concepts like compaction.
* Ability to use from multiple languages, notably Rust and JVM.
* Synchronous file I/O. Given the poor state of AIO on Linux, defer to higher level abstractions to emulate asynchronous behavior. For instance, a dedicated group of threads can be used to interact with Depot.
* Network support is left to higher level libraries.
* Topics, or partitioning of items, may be supported natively in Depot by composing its underlying components. TBD.
* Base91 may be used for encoding data, instead of base64, time permitting.

## FAQ

### How does Depot store data?

Depot stores its data in plain files. Depot's higher level interface composes a *QueueSection*, which stores 65536 items per file.

Depot uses UTF-8 encoded files to store its data. Each entry is delimited by *\n* (0x0A) characters, and each entry consists of its data, encoded as base64, a field separator *\*, and its id data (16 bit integer, big endian). The id is specific to a *QueueSection*, and sections themselves are ordered and assigned ids, which are taken into account when determing an items' 64-bit id value.

### How does Depot deal with crashes while writing data?

When opening the queue for appending, it reads the last character of the file. If it's a newline, the presumption is that the system hasn't crashed.

However, if it isn't a newline (and the file is not empty), Depot assumes that the previous writer has crashed, and it appends a *-* character, 0xTBD, followed by a newline, *\n*. The API allows readers to differentiate between items that were fully written and those that were potentially only partially written. Note that if the system crashes during crash recovery, the corrupted items may accumulate *-* characters.

### How fast is Depot?

That depends on the machine and data size, but it seems to be pretty fast. A Lenovo Thinkpad, i7-6600U, with a consumer-grade SSD achieves over 10 million writes per second, and 5 million reads per second, for small payloads.

Given its append only design, it should also perform well with "spinning rust" disks.

### Does it support multiple concurrent writers?

Multiple concurrent writers are not supported. A library such as [semalock](https://github.com/longshorej/semalock) can be used if coordination between processes is required.

## License

Depot is licensed under the Apache License, Version 2. See [LICENSE](LICENSE).

## Author

Jason Longshore <hello@jasonlongshore.com>
