# cassandra-cluster-copy
Utility for copying data from one Cassandra Cluster Keyspace to another
# Usage
1) Edit `.env` with appropriate connection settings
2) Manually create Keyspace and table schema on destination cluster
3) Run `go run .` answering any missing prompts

# Notes
- Executable can be built using `go build` if portable binary is desired
- SSL is enabled to both source and destination clusters
- Compression is enabled on both clusters
- Up to 10 tables are copied at a time with 1 thread per table
- Current code does not provide progress on the copy. Use `nodetool tablestats` on the destination cluster to monitor
- Tables that are complete are saved to `tables_to_skip.txt`. Each table page state is saved to disk to allow resume on failure.
- Failed queries (read and insert) will not retry and will lead to a fatal error
- Destination columns types MUST match source destination
