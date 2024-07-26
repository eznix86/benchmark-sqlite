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
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            project_id INTEGER,
            user_id INTEGER,
            description TEXT,
            completed BOOLEAN,
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY,
            project_id INTEGER,
            user_id INTEGER,
            content TEXT,
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
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

def seed_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    # Seed projects
    for i in range(1, 1000001):
        c.execute('INSERT INTO projects (name) VALUES (?)', (f'Project {i}',))

    # Seed users
    for i in range(1, 1000001):
        c.execute('INSERT INTO users (name) VALUES (?)', (f'User {i}',))

    # Seed tasks
    for i in range(1, 1000001):
        project_id = random.randint(1, 1000000)
        user_id = random.randint(1, 1000000)
        c.execute('INSERT INTO tasks (project_id, user_id, description, completed) VALUES (?, ?, ?, ?)',
                  (project_id, user_id, f'Task {i}', False))

    # Seed notes
    for i in range(1, 1000001):
        project_id = random.randint(1, 1000000)
        user_id = random.randint(1, 1000000)
        c.execute('INSERT INTO notes (project_id, user_id, content) VALUES (?, ?, ?)',
                  (project_id, user_id, f'Note {i}'))

    conn.commit()
    conn.close()

def get_connection():
    if not hasattr(thread_local, "connection"):
        thread_local.connection = sqlite3.connect(DB_NAME, check_same_thread=False)
    return thread_local.connection

def optimize_db(c, optimize_wal):
    if optimize_wal:
        c.execute('PRAGMA synchronous = NORMAL')
        c.execute('PRAGMA busy_timeout = 10000')
        c.execute('PRAGMA cache_size = 4096')
        c.execute('PRAGMA temp_store = MEMORY')
    pass

def read_operation(optimize_wal):
    start_time = time.time()
    conn = get_connection()
    try:
        c = conn.cursor()
        optimize_db(c, optimize_wal)
        table = random.choice(['projects', 'users', 'tasks', 'notes'])
        c.execute(f'SELECT * FROM {table} WHERE id = ?', (random.randint(1, 1000000),))
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
        operation = random.choice(['insert', 'update', 'delete'])
        if operation == 'insert':
            table = random.choice(['projects', 'users', 'tasks', 'notes'])
            if table == 'projects':
                c.execute('INSERT INTO projects (name) VALUES (?)', (f'New Project {random.randint(1, 1000000)}',))
            elif table == 'users':
                c.execute('INSERT INTO users (name) VALUES (?)', (f'New User {random.randint(1, 1000000)}',))
            elif table == 'tasks':
                project_id = random.randint(1, 1000000)
                user_id = random.randint(1, 1000000)
                c.execute('INSERT INTO tasks (project_id, user_id, description, completed) VALUES (?, ?, ?, ?)',
                          (project_id, user_id, f'New Task {random.randint(1, 1000000)}', False))
            elif table == 'notes':
                project_id = random.randint(1, 1000000)
                user_id = random.randint(1, 1000000)
                c.execute('INSERT INTO notes (project_id, user_id, content) VALUES (?, ?, ?)',
                          (project_id, user_id, f'New Note {random.randint(1, 1000000)}'))
        elif operation == 'update':
            table = random.choice(['tasks'])
            if table == 'tasks':
                task_id = random.randint(1, 1000000)
                c.execute('UPDATE tasks SET completed = ? WHERE id = ?', (True, task_id))
        elif operation == 'delete':
            table = random.choice(['projects', 'users', 'tasks', 'notes'])
            if table == 'projects':
                project_id = random.randint(1, 1000000)
                c.execute('DELETE FROM projects WHERE id = ?', (project_id,))
            elif table == 'users':
                user_id = random.randint(1, 1000000)
                c.execute('DELETE FROM users WHERE id = ?', (user_id,))
            elif table == 'tasks':
                task_id = random.randint(1, 1000000)
                c.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
            elif table == 'notes':
                note_id = random.randint(1, 1000000)
                c.execute('DELETE FROM notes WHERE id = ?', (note_id,))
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
    seed_db()  # Seed the database with initial data

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
