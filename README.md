# Benchmark SQLite

This is a tool to benchmark SQLite. It is written in Python and uses the `sqlite3` module.
This is mostly a playground to know how SQLite behaves under different conditions and play around with the WAL mode.
This should not be used as a reference for any production code.
But, if you want to compare environments, this could be a good starting point. Example;
- Running the benchmark on a local machine vs a cloud instance.
- Running the benchmark with WAL mode vs without WAL mode.
- Running the benchmark on different storage pools. Example Ceph vs Local Disk vs NFS.
- Running the benchmark with different configurations. Example; WAL optimization, cache size, etc.
- Running the benchmark with different versions of SQLite.

Again, this is not a reference for any production code. This is just a playground to understand SQLite better.

## Contributing

If you want to contribute, feel free to open a PR.

## Usage

```bash
poetry install # Install dependencies

# Run the benchmark
python bench.py --clients 25 --queries 15000 --write-percentage 0.5
python bench.py --clients 25 --queries 15000 --write-percentage 0.5 --optimized

# old
python benchmark.py --clients 25 --queries 15000 --write-percentage 0.5 --wal --wal-optimize

# Run the benchmark with a seed
# old
python benchmark-with-seed.py --clients 25 --queries 15000 --write-percentage 0.5 --wal --wal-optimize
```

### Notes

- The `--wal` flag enables WAL mode.
- The `--wal-optimize` flag enables WAL optimization (you can play around with the config)
- The `--clients` flag is the number of clients that will be running queries concurrently.
- The `--queries` flag is the number of queries each client will run.
- The `--write-percentage` flag is the percentage of write queries. The rest will be read queries. Example 0.3 means 30% of the queries will be write queries.
