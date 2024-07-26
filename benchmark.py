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


def init_db(wal_mode=False):

    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS test (
            id INTEGER PRIMARY KEY,
            data TEXT
        )
    ''')
    c.execute('VACUUM')
    c.execute('PRAGMA optimize')
    if wal_mode:
        c.execute('PRAGMA journal_mode = WAL')
    else:
        c.execute('PRAGMA journal_mode = DELETE')
    conn.commit()
    conn.close()

def get_connection():
    if not hasattr(thread_local, "connection"):
        thread_local.connection = sqlite3.connect(DB_NAME, check_same_thread=False)
    return thread_local.connection

def optimize_db(c, optimize_wal):
    if optimize_wal:
        c.execute('PRAGMA synchronous = NORMAL')
        c.execute('PRAGMA busy_timeout = 5000')
        c.execute('PRAGMA cache_size = 4096')
        c.execute('PRAGMA temp_store = MEMORY')
    pass

def read_operation(optimize_wal):
    start_time = time.time()
    conn = get_connection()
    try:
        c = conn.cursor()
        optimize_db(c, optimize_wal)
        c.execute('SELECT * FROM test WHERE id = ?', (random.randint(1, 1000000),))
        result = c.fetchone()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    duration = time.time() - start_time
    return duration, 'read'

def write_operation(optimize_wal):
    start_time = time.time()
    conn = get_connection()
    try:
        c = conn.cursor()
        optimize_db(c, optimize_wal)
        data = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        c.execute('INSERT INTO test (data) VALUES (?)', (data,))
        conn.commit()
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    duration = time.time() - start_time
    return duration, 'write'

def perform_query(write_percentage, optimize_wal):
    if random.random() < write_percentage:
        return write_operation(optimize_wal)
    else:
        return read_operation(optimize_wal)

def client_worker(client_id, num_queries, write_percentage, results, progress_bars, start_time, optimize_wal):

    while time.time() < start_time:
        pass

    pbar = progress_bars[client_id]
    client_results = []
    for _ in range(num_queries):
        result = perform_query(write_percentage, optimize_wal)
        client_results.append(result)
        pbar.update(1)
    results[client_id] = client_results

def run_queries(num_clients, num_queries, write_percentage, start_time, optimize_wal):
    results = [None] * num_clients
    # queries_per_client = num_queries // num_clients
    # remaining_queries = num_queries % num_clients

    queries_per_client = num_queries
    remaining_queries = num_queries
    progress_bars = [tqdm(total=queries_per_client + (1 if i < remaining_queries else 0),
                          desc=f"Client {i+1}", position=i)
                     for i in range(num_clients)]

    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = []
        for i in range(num_clients):
            client_queries = queries_per_client + (1 if i < remaining_queries else 0)
            futures.append(executor.submit(client_worker, i, client_queries, write_percentage, results, progress_bars, start_time, optimize_wal))

        for future in as_completed(futures):
            future.result()

    for pbar in progress_bars:
        pbar.close()

    return [item for sublist in results for item in sublist]

def calculate_percentiles(durations, percentiles=[99.9, 99, 95, 90, 50]):
    return {p: np.percentile(durations, p) * 1000 for p in percentiles}  # Convert to milliseconds

def warm_up(num_queries, optimize_wal):
    print("Warming up...")
    for _ in tqdm(range(num_queries), desc="Warm-up"):
        perform_query(0.5, optimize_wal)  # 50% write operations during warm-up

def main():
    parser = argparse.ArgumentParser(description="Run parallel SQLite queries with read/write ratio control")
    parser.add_argument('--clients', type=int, default=10, help='Number of clients')
    parser.add_argument('--queries', type=int, default=100, help='Number of queries')
    parser.add_argument('--write-percentage', type=float, default=0.3, help='Percentage of write operations (0 to 1)')
    parser.add_argument('--warm-up', type=int, default=1000, help='Number of warm-up queries')
    parser.add_argument('--wal', action='store_true', help='Enable WAL mode')
    parser.add_argument('--wal-optimize', action='store_true',help='Optimize WAL mode')

    args = parser.parse_args()

    num_clients = args.clients
    num_queries = args.queries
    write_percentage = args.write_percentage
    warm_up_queries = args.warm_up
    wal_mode = args.wal
    wal_optimize = args.wal_optimize

    init_db(wal_mode)  # Initialize the database

    optimize_wal = args.wal_optimize and wal_mode

    warm_up(warm_up_queries, optimize_wal)  # Perform warm-up

    print(f"\nStarting benchmark with {num_clients} clients, {num_queries} queries, and {write_percentage:.1%} write percentage")
    start_time = time.time() + 3  # Wait for a few seconds before starting the benchmark

    results = run_queries(num_clients, num_queries, write_percentage, start_time, optimize_wal)
    end_time = time.time()

    total_duration = end_time - start_time
    total_reads = len([r for r in results if r[1] == 'read'])
    total_writes = len([r for r in results if r[1] == 'write'])
    read_durations = [r[0] for r in results if r[1] == 'read']
    write_durations = [r[0] for r in results if r[1] == 'write']

    read_percentiles = calculate_percentiles(read_durations)
    write_percentiles = calculate_percentiles(write_durations)

    print(f'\nTotal queries: {num_queries}')
    print(f'Total reads: {total_reads}')
    print(f'Total writes: {total_writes}')
    print(f'Total duration: {total_duration:.2f} seconds')
    print(f'Queries per second: {num_queries / total_duration:.2f}')
    print(f'Reads per second: {total_reads / total_duration:.2f}')
    print(f'Writes per second: {total_writes / total_duration:.2f}')

    table = [[f"P{p}", f"{write_percentiles[p]:.3f}", f"{read_percentiles[p]:.3f}"] for p in sorted(read_percentiles.keys(), reverse=True)]
    print('\nPercentiles (milliseconds):')
    print(tabulate(table, headers=['Percentile', 'Write', 'Read'], tablefmt='grid'))

    # Add average (mean) durations
    avg_write = np.mean(write_durations) * 1000  # Convert to milliseconds
    avg_read = np.mean(read_durations) * 1000    # Convert to milliseconds
    print(f"\nAverage durations (milliseconds):")
    print(f"Write: {avg_write:.3f}")
    print(f"Read: {avg_read:.3f}")


if __name__ == '__main__':
    main()
