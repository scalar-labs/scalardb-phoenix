# scalardb-phoenix

A storage implementation of [Scalar DB](https://github.com/scalar-labs/scalardb/) using [Apache Phoenix](http://phoenix.apache.org/). Apache Phoenix is an add-on for [Apache HBase](https://hbase.apache.org/) that provides a programmatic ANSI SQL interface.

## Limitation

- The atomic unit of Scalar DB with the Cassandra storage implementation (default) is a partition. However, the atomic unit of this storage implementation is PartitionKey + ClusteringKey, which is difference from the default storage implementation (Cassandra).
- Conditional update with multiple conditions is not supported.
