# chukcha
Easy to use distributed event bus similar to Kafka.

The event bus is designed to be used as a persistent intermediate storage buffer for any kinds of events or logs that you might want.

Possible usages for Chukcha are basically the same as for Kafka, including, but not limited to:

 - Centralised logs collection,
 - Collecting structured or unstructured events for machine learning or analytics (e.g. Hadoop, ClickHouse, etc),
 - Change log buffer to synchronise the contents of one or more storages with each other (e.g. update a full-text index upon database update).

The messages are expected to be delimited by a new line character, although there are no other limitations in terms of events format: it can be JSON, plain text, Protobuf (with `\` and new line character escaped), etc.

The youtube playlist to see live development: https://www.youtube.com/watch?v=t3FdULDRfRM&list=PLWwSgbaBp9XqeuIuTWqpNtvf_EL0I4TJ2

# Requirements

Go 1.16+ is needed to build Chukcha.

# Features (work in progress)

1. Easy to configure out of the box to not lose data unexpectedly.
2. Distributed, with asynchronous replication by default.
3. Explicit acknowledgement of data that was read.

# Limitations

1. The maximum supported size is 1 MiB per message.
2. Each message has to be separated by a new line character (`\n`).
3. The maximum default write batch size is 4 MiB.
4. Maximum memory consumption is around `(write batch size) * 2` (8 MiB with default settings) per connection. TODO: memory consumption per connection can be brought down significantly if using a custom protocol on top of HTTP.
5. The default upper bound for the number of open connections is 262144.
6. Kafka protocol is not likely to be ever supported unless someone decides to contribute it.

# Data loss scenarios

By default the data is not flushed to disk upon each write, but this can be changed in the server config or per request.

The data durability guarantees are thus the following:

1. By default data written to a single node can be lost, if:
 - The machine was not shut down cleanly, e.g. a kernel panic occurred or a host experienced any other hardware failures, or
 - The machine was completely lost and never returned into the cluster
2. (TODO): When `min_sync_replicas` is greater than 0, data will only be lost if:
 - More than the `min_sync_replicas` machines are not shut down cleanly simultaneously, or
 - More than `min_sync_replicas` machines are lost completely and never returned to the cluster.
3. (TODO2): When `sync_each_write=true` is set, data is only lost when machine's hard drives/SSDs fail.
4. (TODO2): When both `sync_each_write=true` is set and `min_sync_replicas` is greater than zero, the data is only lost if more than `min_sync_replicas` machines are totally lost and never re-join the cluster.

# Design (work in progress)

1. Data is split into chunks and is stored as files on disk. Each server owns the chunks being written into and the ownership of each individual chunk is never changed. The files are replicated to all other servers in the cluster.
2. Readers explicitly acknowledge data that was read and processed successfully. Readers are responsible for reading the chunks starting with the appropirate offsets. Data can expire after a set period of time if so desired (TODO). By default data does not expire at all.

# Replication

1. Every file in data directory looks like the following: `<category>/<server_name>-chunkXXXXXXXXX`.
2. Each instance in the cluster must have a unique name and it will be used as a prefix to all files in the category.
3. Clients will only connect to a single instance and consume chunks from all the servers that has been downloaded to the current instance.

# Installation

1. Install Go at https://golang.org/
2. Run the following command in your terminal: `$ go install -v github.com/YuriyNasretdinov/chukcha@latest`
3. The binary built should be located at `~/go/bin/chukcha` or at `$GOPATH/bin/chukcha` if `$GOPATH` was set.

# Usage

It's too early to start using it yet! If you really want to, you can, of course, but keep in mind that this is a project still in the early days of it's development and it's API will probably change a lot before stabilising.

If you really want to use Chukcha, please refer to the simple Go client library at https://pkg.go.dev/github.com/YuriyNasretdinov/chukcha/client.

# TODOs:

1. Limit for the maximum message size is 1 MiB, otherwise we can no longer serve results from disk because we read from disk in 1 MiB chunks.
1. Handle situations when we run out of disk space or the number of inodes.
1. Compute code coverage.
1. Introduce replication.
1. Write replication tests
1. Describe the data model.
