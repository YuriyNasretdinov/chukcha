# chukcha
Easy to use distributed event bus similar to Kafka

# Features (work in progress)

1. Easy to configure out of the box to not lose data.
2. Distributed, with asynchronous replication by default.
3. Explicit acknowledgement of data that was read.

# Design (work in progress)

1. Data is split into chunks and is stored as files on disk. The files are replicated.
2. Readers explicitly acknowledge data that was read. Readers are responsible for reading the chunks starting with the appropirate offsets.

# TODOs:

1. Limit for the maximum message size is 1 MiB, otherwise we can no longer serve results from disk because we read from disk in 1 MiB chunks.
2. Write a more fine-grained test for on-disk format.
