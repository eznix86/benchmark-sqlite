import sqlite3
import random
import string
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import numpy as np
from tabulate import tabulate
import threading
import os

DB_NAME = 'test.db'
thread_local = threading.local()

def init_db(optimized=False):
    """Initialize the database with the specified settings."""
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    if optimized:
        # Apply persistent optimizations
        cursor.execute('PRAGMA auto_vacuum = INCREMENTAL')
        cursor.execute('VACUUM')
        cursor.execute('PRAGMA journal_mode = DELETE')
        cursor.execute('PRAGMA page_size = 32768')
        cursor.execute('VACUUM')
        cursor.execute('PRAGMA journal_mode = WAL')
    else:
        # Default settings
        cursor.execute('PRAGMA journal_mode = DELETE')

    # Create test table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS test (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
    ''')

    conn.commit()
    conn.close()

def get_connection(optimized=False):
    """Get a database connection for the current thread."""
    if not hasattr(thread_local, "connection"):
        isolation_level = 'EXCLUSIVE' if optimized else None;
        thread_local.connection = sqlite3.connect(DB_NAME, check_same_thread=False, isolation_level=isolation_level)

        if optimized:
            # Apply runtime optimizations
            cursor = thread_local.connection.cursor()
            cursor.execute('PRAGMA cache_size = 1000000000')
            cursor.execute('PRAGMA mmap_size = 2147483648')
            cursor.execute('PRAGMA temp_store = MEMORY')
            cursor.execute('PRAGMA synchronous = NORMAL')
            cursor.execute('PRAGMA busy_timeout = 10000')

    return thread_local.connection

def read_operation(optimized=False):
    """Perform a random read operation."""
    start_time = time.time()
    conn = get_connection(optimized)

    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM test WHERE id = ?', (random.randint(1, 1000000),))
        result = cursor.fetchone()
    except sqlite3.Error as e:
        print(f"Read error: {e}")

    duration = time.time() - start_time
    return duration, 'read'

def write_operation(optimized=False):
    """Perform a write operation with random data."""
    start_time = time.time()
    conn = get_connection(optimized)

    try:
        cursor = conn.cursor()
        data = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        cursor.execute('INSERT INTO test (data) VALUES (?)', (data,))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Write error: {e}")

    duration = time.time() - start_time
    return duration, 'write'

def perform_query(write_percentage, optimized=False):
    """Perform either a read or write query based on the write percentage."""
    if random.random() < write_percentage:
        return write_operation(optimized)
    else:
        return read_operation(optimized)

def client_worker(client_id, num_queries, write_percentage, results, progress_bars, start_time, optimized=False):
    """Worker function for each client thread."""
    # Wait for synchronized start
    while time.time() < start_time:
        pass

    pbar = progress_bars[client_id]
    client_results = []

    for _ in range(num_queries):
        result = perform_query(write_percentage, optimized)
        client_results.append(result)
        pbar.update(1)

    results[client_id] = client_results

def run_benchmark(num_clients, num_queries, write_percentage, optimized=False):
    """Run the benchmark with the specified parameters."""
    # Initialize the database
    init_db(optimized)

    # Perform warm-up
    print("Warming up...")
    warm_up_queries = 1000
    for _ in tqdm(range(warm_up_queries), desc="Warm-up"):
        perform_query(0.5, optimized)  # 50% write operations during warm-up

    # Prepare for the benchmark
    print(f"\nStarting benchmark with {num_clients} clients, {num_queries} queries per client, and {write_percentage:.1%} write percentage")
    start_time = time.time() + 3  # Wait for a few seconds before starting the benchmark

    # Initialize results and progress bars
    results = [None] * num_clients
    progress_bars = [tqdm(total=num_queries, desc=f"Client {i+1}", position=i) for i in range(num_clients)]

    # Run the benchmark with multiple clients
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = []
        for i in range(num_clients):
            futures.append(executor.submit(
                client_worker, i, num_queries, write_percentage, results, progress_bars, start_time, optimized
            ))

        for future in as_completed(futures):
            future.result()

    # Close progress bars
    for pbar in progress_bars:
        pbar.close()

    # Flatten the results
    flattened_results = [item for sublist in results if sublist for item in sublist]
    benchmark_end_time = time.time()

    return flattened_results, start_time, benchmark_end_time

def calculate_percentiles(durations, percentiles=[99.9, 99, 95, 90, 50]):
    """Calculate percentiles for the given durations."""
    return {p: np.percentile(durations, p) * 1000 for p in percentiles}  # Convert to milliseconds

def print_results(results, start_time, end_time):
    """Print benchmark results and statistics."""
    total_duration = end_time - start_time
    total_queries = len(results)
    total_reads = len([r for r in results if r[1] == 'read'])
    total_writes = len([r for r in results if r[1] == 'write'])

    read_durations = [r[0] for r in results if r[1] == 'read']
    write_durations = [r[0] for r in results if r[1] == 'write']

    read_percentiles = calculate_percentiles(read_durations) if read_durations else {}
    write_percentiles = calculate_percentiles(write_durations) if write_durations else {}

    print(f'\nTotal queries: {total_queries}')
    print(f'Total reads: {total_reads}')
    print(f'Total writes: {total_writes}')
    print(f'Total duration: {total_duration:.2f} seconds')
    print(f'Queries per second: {total_queries / total_duration:.2f}')
    print(f'Reads per second: {total_reads / total_duration:.2f}')
    print(f'Writes per second: {total_writes / total_duration:.2f}')

    # Print percentiles
    table = []
    for p in sorted([99.9, 99, 95, 90, 50], reverse=True):
        write_value = f"{write_percentiles.get(p, 0):.3f}" if write_durations else "N/A"
        read_value = f"{read_percentiles.get(p, 0):.3f}" if read_durations else "N/A"
        table.append([f"P{p}", write_value, read_value])

    print('\nPercentiles (milliseconds):')
    print(tabulate(table, headers=['Percentile', 'Write', 'Read'], tablefmt='grid'))

    # Print average durations
    avg_write = np.mean(write_durations) * 1000 if write_durations else 0  # Convert to milliseconds
    avg_read = np.mean(read_durations) * 1000 if read_durations else 0     # Convert to milliseconds

    print(f"\nAverage durations (milliseconds):")
    print(f"Write: {avg_write:.3f}")
    print(f"Read: {avg_read:.3f}")

def main():
    """Main function to handle command-line arguments and run the benchmark."""
    parser = argparse.ArgumentParser(description="SQLite Benchmark Tool")
    parser.add_argument('--clients', type=int, default=25,
                        help='Number of concurrent clients (default: 25)')
    parser.add_argument('--queries', type=int, default=15000,
                        help='Number of queries per client (default: 15000)')
    parser.add_argument('--write-percentage', type=float, default=0.5,
                        help='Percentage of write operations between 0 and 1 (default: 0.5)')
    parser.add_argument('--optimized', action='store_true',
                        help='Enable SQLite optimizations')

    args = parser.parse_args()

    # Validate input parameters
    if args.clients <= 0:
        parser.error("Number of clients must be positive")
    if args.queries <= 0:
        parser.error("Number of queries must be positive")
    if not 0 <= args.write_percentage <= 1:
        parser.error("Write percentage must be between 0 and 1")

    # Run the benchmark
    results, start_time, end_time = run_benchmark(
        args.clients,
        args.queries,
        args.write_percentage,
        args.optimized
    )

    # Print results
    print_results(results, start_time, end_time)

if __name__ == '__main__':
    main()
