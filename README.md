# chukcha
Easy to use distributed event bus similar to Kafka.

The messages are expected to be delimited by a new line character.

The youtube playlist to see live development: https://www.youtube.com/watch?v=t3FdULDRfRM&list=PLWwSgbaBp9XqeuIuTWqpNtvf_EL0I4TJ2

# Requirements

Go 1.15+ is needed to build Chukcha.

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
3. Handle situations when we run out of disk space or the number of inodes.
