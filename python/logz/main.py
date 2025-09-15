#!/usr/bin/env python
import asyncio
import concurrent.futures
import boto3
import json
import time
import argparse
import os
import hashlib
import threading
from datetime import datetime
from typing import List, Dict, Any
from cachetools import LRUCache
from dataclasses import dataclass

# Configuration - hardcoded values as requested
# AWS_ACCOUNT_ID = "123456"  # Replace with your AWS account ID

# Query configuration
MAX_LINES_PER_FILE = 5000
OUTPUT_DIR = ".output"
MAX_HASH_CACHE_SIZE = 30000  # Maximum number of row hashes to keep in LRU cache

class HashDeduplicator:
    def __init__(self, max_size: int = MAX_HASH_CACHE_SIZE):
        """Initialize the hash deduplicator using cachetools.LRUCache."""
        self.cache = LRUCache(maxsize=max_size)
        self.hits = 0
        self.misses = 0
        self.max_size = max_size
        self._lock = threading.Lock()  # Thread-safe access to cache and counters
    
    def contains(self, row_hash: str) -> bool:
        """Check if hash exists in cache and move to front if found (LRU behavior)."""
        with self._lock:
            if row_hash in self.cache:
                # Access the value to trigger LRU update
                _ = self.cache[row_hash]
                self.hits += 1
                return True
            else:
                self.misses += 1
                return False
    
    def add(self, row_hash: str) -> None:
        """Add a new hash to the cache, removing oldest if at capacity."""
        with self._lock:
            self.cache[row_hash] = True
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'hits': self.hits,
                'misses': self.misses,
                'total_checks': self.hits + self.misses,
                'hit_rate': self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0.0
            }

class StreamingFileWriter:
    def __init__(self, base_filename: str, max_lines_per_file: int = MAX_LINES_PER_FILE):
        """Initialize the streaming file writer."""
        self.base_filename = base_filename
        self.max_lines_per_file = max_lines_per_file
        self.current_file_index = 1
        self.current_line_count = 0
        self.current_file = None
        self.files_created = []
        self._lock = threading.Lock()  # Thread-safe access to file operations and shared state
        
        # Ensure output directory exists
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
    
    def _open_new_file(self):
        """Open a new output file. Must be called with lock held."""
        if self.current_file:
            self.current_file.close()
        
        filename = f"{self.base_filename}_{self.current_file_index:03d}.jsonl"
        filepath = os.path.join(OUTPUT_DIR, filename)
        self.current_file = open(filepath, 'w')
        self.files_created.append(filepath)
        self.current_line_count = 0
        self.current_file_index += 1
        
        print(f"Writing to file: {filepath}")
    
    def write_entry(self, entry: str):
        """Write a single entry to the current file."""
        with self._lock:
            # Check if we need to start a new file
            if self.current_file is None or self.current_line_count >= self.max_lines_per_file:
                self._open_new_file()
            
            # Write the entry
            self.current_file.write(entry + '\n')
            self.current_file.flush()  # Ensure data is written immediately
            self.current_line_count += 1
    
    def close(self):
        """Close the current file."""
        with self._lock:
            if self.current_file:
                self.current_file.close()
                self.current_file = None
    
    def get_files_created(self) -> List[str]:
        """Get list of files created."""
        with self._lock:
            return self.files_created.copy()  # Return a copy to avoid external modification

class CloudWatchLogsQuerier:
    def __init__(self, region: str = 'eu-west-1', enable_deduplication: bool = False):
        """Initialize the CloudWatch Logs client."""
        self.client = boto3.client('logs', region_name=region)
        self.region = region
        self.enable_deduplication = enable_deduplication
        self.hash_cache = HashDeduplicator() if enable_deduplication else None
        self.duplicates_found = 0
        self._duplicates_lock = threading.Lock()  # Thread-safe access to duplicates counter
        
    def start_query(self, log_groups: List[str], query: str, start_time: int, end_time: int) -> str:
        """Start a CloudWatch Logs Insights query."""
        try:
            response = self.client.start_query(
                logGroupNames=log_groups,
                startTime=start_time,
                endTime=end_time,
                queryString=query
            )
            return response['queryId']
        except Exception as e:
            print(f"Error starting query: {e}")
            raise
    
    def get_query_results(self, query_id: str) -> Dict[str, Any]:
        """Get the results of a CloudWatch Logs Insights query."""
        try:
            response = self.client.get_query_results(queryId=query_id)
            return response
        except Exception as e:
            print(f"Error getting query results: {e}")
            raise
    
    def _generate_row_hash(self, formatted_entry: str) -> str:
        """Generate a SHA-256 hash for a row entry."""
        return hashlib.sha256(formatted_entry.encode('utf-8')).hexdigest()
    
    def format_log_entry(self, result: List[Dict[str, str]]) -> str:
        """Format a single log entry for output as single-line JSON, filtering out @ptr fields."""
        entry = {}
        for field in result:
            field_name = field['field']
            # Skip @ptr fields
            if field_name == '@ptr':
                continue
            entry[field_name] = field['value']
        
        return json.dumps(entry, separators=(',', ':'))  # Compact JSON format
    
    def stream_query_results(self, log_groups: List[str], query: str, start_time: int, end_time: int, 
                           file_writer: StreamingFileWriter) -> int:
        """Execute the logs query and stream results to file as they arrive."""
        print(f"Starting query for log groups: {log_groups}")
        print(f"Query: {query}")
        print(f"Time range: {datetime.fromtimestamp(start_time)} to {datetime.fromtimestamp(end_time)}")
        
        query_id = self.start_query(log_groups, query, start_time, end_time)
        print(f"Query ID: {query_id}")
        
        total_entries = 0
        start_wait = time.time()
        max_wait_time = 300  # 5 minutes timeout
        
        while time.time() - start_wait < max_wait_time:
            results = self.get_query_results(query_id)
            status = results['status']
            
            print(f"Query status: {status}, entries so far: {total_entries}")
            
            # Process any new results
            if 'results' in results:
                for result in results['results']:
                    formatted_entry = self.format_log_entry(result)
                    if formatted_entry.strip() != '{}':  # Skip empty entries
                        
                        # Only perform deduplication if enabled
                        if self.enable_deduplication:
                            # Generate hash for deduplication
                            row_hash = self._generate_row_hash(formatted_entry)
                            
                            # Check if we've seen this row before
                            if self.hash_cache.contains(row_hash):
                                # Duplicate found, skip writing
                                with self._duplicates_lock:
                                    self.duplicates_found += 1
                                continue
                            
                            # New row, add to cache
                            self.hash_cache.add(row_hash)
                        
                        # Write to file
                        file_writer.write_entry(formatted_entry)
                        total_entries += 1
                        
                        # Progress indicator for large datasets
                        if total_entries % 10000 == 0:
                            if self.enable_deduplication:
                                cache_stats = self.hash_cache.get_stats()
                                with self._duplicates_lock:
                                    duplicates_count = self.duplicates_found
                                print(f"Processed {total_entries} entries, duplicates skipped: {duplicates_count}, cache hit rate: {cache_stats['hit_rate']:.2%}")
                            else:
                                print(f"Processed {total_entries} entries (deduplication disabled)")
            
            if status == 'Complete':
                print(f"Query completed. Total entries processed: {total_entries}")
                return total_entries
            elif status == 'Failed':
                raise Exception("Query failed")
            elif status == 'Cancelled':
                raise Exception("Query was cancelled")
            
            time.sleep(2)  # Wait 2 seconds before checking again
        
        raise Exception(f"Query timed out after {max_wait_time} seconds")


def parse_datetime(date_string: str) -> datetime:
    """Parse datetime string in various formats."""
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%Y/%m/%d %H:%M:%S',
        '%Y/%m/%d %H:%M',
        '%Y/%m/%d',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime: {date_string}")

def generate_time_chunks(start_time: int, end_time: int, chunk_minutes: int) -> List[tuple]:
    """Generate time chunks for processing."""
    chunks = []
    current_start = start_time
    chunk_seconds = chunk_minutes * 60
    
    while current_start < end_time:
        current_end = min(current_start + chunk_seconds, end_time)
        chunks.append((current_start, current_end))
        current_start = current_end
    
    return chunks

@dataclass
class ConsumerPayload:
    chunk_start: int
    chunk_end: int
    log_groups: List[str]
    query: str
    querier: CloudWatchLogsQuerier
    file_writer: StreamingFileWriter

@dataclass
class ProducerPayload:
    log_groups: List[str]
    query: str
    start_timestamp: int
    end_timestamp: int
    chunk_minutes: int
    querier: CloudWatchLogsQuerier
    file_writer: StreamingFileWriter

def process_payload(payload: ConsumerPayload) -> int:
    """Process a payload by executing the query and streaming results to file."""
    print(f"Processing payload: {payload}")

    # Execute the query and stream results to file
    total_entries = payload.querier.stream_query_results(
        payload.log_groups,
        payload.query,
        payload.chunk_start,
        payload.chunk_end,
        payload.file_writer
    )

    return total_entries


async def producer(q: asyncio.Queue, producer_payload: ProducerPayload):
    """Producer function that creates payloads and adds them to the queue."""
    print("[Producer] Starting to generate payloads")
    
    # Generate time chunks
    time_chunks = generate_time_chunks(
        producer_payload.start_timestamp, 
        producer_payload.end_timestamp, 
        producer_payload.chunk_minutes
    )
    total_chunks = len(time_chunks)
    
    print(f"[Producer] Generated {total_chunks} time chunks of {producer_payload.chunk_minutes} minutes each")
    
    # Create and queue payloads for each time chunk
    for chunk_index, (chunk_start, chunk_end) in enumerate(time_chunks, 1):
        chunk_start_dt = datetime.fromtimestamp(chunk_start)
        chunk_end_dt = datetime.fromtimestamp(chunk_end)
        
        print(f"[Producer] Queuing chunk {chunk_index}/{total_chunks}: {chunk_start_dt} to {chunk_end_dt}")
        
        payload = ConsumerPayload(
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            log_groups=producer_payload.log_groups,
            query=producer_payload.query,
            querier=producer_payload.querier,
            file_writer=producer_payload.file_writer
        )
        
        await q.put(payload)
    
    print(f"[Producer] All {total_chunks} payloads queued successfully")


async def consumer(consumer_id: int, q: asyncio.Queue, executor: concurrent.futures.Executor):
    """Consumer function that processes payloads from the queue."""
    loop = asyncio.get_event_loop()
    processed_count = 0

    print(f"[Consumer {consumer_id}] Starting up")

    while True:
        payload: ConsumerPayload = await q.get()
        if payload is None:  # poison pill
            q.task_done()
            print(f"[Consumer {consumer_id}] Shutting down after processing {processed_count} payloads")
            break

        chunk_start_dt = datetime.fromtimestamp(payload.chunk_start)
        chunk_end_dt = datetime.fromtimestamp(payload.chunk_end)
        print(f"[Consumer {consumer_id}] Processing chunk: {chunk_start_dt} to {chunk_end_dt}")

        try:            
            # Run the blocking function in a separate thread
            entries = await loop.run_in_executor(executor, process_payload, payload)
            processed_count += 1

            print(f"[Consumer {consumer_id}] Finished processing chunk. Entries: {entries}, Total processed: {processed_count}")
        except Exception as e:
            print(f"[Consumer {consumer_id}] Error processing payload: {e}")
        
        q.task_done()


async def main():
    parser = argparse.ArgumentParser(description='Query AWS CloudWatch Logs using Logs Insights with Producer-Consumer Pattern')
    parser.add_argument('--start-time', required=True, 
                       help='Start time (format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)')
    parser.add_argument('--end-time', required=True,
                       help='End time (format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD)')
    parser.add_argument('--query', required=True,
                       help='CloudWatch Logs Insights query string')
    parser.add_argument('--log-groups', nargs='+', required=True,
                       help='One or more CloudWatch log group names to query')
    parser.add_argument('--region', default='eu-west-1',
                       help='AWS region (default: eu-west-1)')
    parser.add_argument('--output-prefix', default='cloudwatch_logs',
                       help='Prefix for output files (default: cloudwatch_logs)')
    parser.add_argument('--chunk-minutes', type=int, default=60,
                       help='Time chunk size in minutes (default: 60)')
    parser.add_argument('--enable-deduplication', action='store_true',
                       help='Enable row deduplication using LRU cache (default: disabled)')
    parser.add_argument('--num-consumers', type=int, default=12,
                       help='Number of consumer workers (default: 12)')
    parser.add_argument('--queue-size', type=int, default=100,
                       help='Maximum queue size for backpressure (default: 100)')
    parser.add_argument('--max-workers', type=int, default=8,
                       help='Maximum number of threads in ThreadPoolExecutor (default: 8)')
    
    args = parser.parse_args()

    # Parse start and end times
    start_dt = parse_datetime(args.start_time)
    end_dt = parse_datetime(args.end_time)
    
    # Convert to Unix timestamps
    start_timestamp = int(start_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())
    
    print("=== CloudWatch Logs Producer-Consumer Setup ===")
    print(f"Querying logs from {start_dt} to {end_dt}")
    print(f"Using log groups: {args.log_groups}")
    print(f"AWS Region: {args.region}")
    print(f"Chunk size: {args.chunk_minutes} minutes")
    print(f"Consumers: {args.num_consumers}")
    print(f"Queue size: {args.queue_size}")
    print(f"Max worker threads: {args.max_workers}")
    print(f"Deduplication: {'enabled' if args.enable_deduplication else 'disabled'}")
    
    # Initialize the querier and file writer
    querier = CloudWatchLogsQuerier(args.region, enable_deduplication=args.enable_deduplication)
    file_writer = StreamingFileWriter(args.output_prefix)
    
    # Create ProducerPayload structure
    producer_payload = ProducerPayload(
        log_groups=args.log_groups,
        query=args.query,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        chunk_minutes=args.chunk_minutes,
        querier=querier,
        file_writer=file_writer
    )
    
    # Create asyncio queue with specified size for backpressure
    queue = asyncio.Queue(maxsize=args.queue_size)
    
    # Create ThreadPoolExecutor for blocking I/O operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        try:
            print("\n=== Starting Producer-Consumer Processing ===")
            
            # Create consumer tasks
            consumer_tasks = []
            for i in range(args.num_consumers):
                task = asyncio.create_task(consumer(i + 1, queue, executor))
                consumer_tasks.append(task)
            
            # Create producer task with new ProducerPayload structure
            producer_task = asyncio.create_task(
                producer(queue, producer_payload)
            )
            
            # Wait for producer to finish
            await producer_task
            print("[Main] Producer finished, sending poison pills to consumers...")
            
            # Send poison pills to signal consumers to shut down
            for i in range(args.num_consumers):
                await queue.put(None)
                print(f"[Main] Sent poison pill {i+1}/{args.num_consumers}")
            
            print("[Main] All poison pills sent, waiting for consumers to complete...")
            
            # Wait for all items in queue to be processed
            await queue.join()
            print("[Main] All queue items processed")
            
            # Wait for all consumer tasks to complete
            await asyncio.gather(*consumer_tasks)
            print("[Main] All consumers shut down")
            
            print("\n=== Processing Complete ===")
            
            # Print final statistics
            if querier.enable_deduplication:
                cache_stats = querier.hash_cache.get_stats()
                with querier._duplicates_lock:
                    final_duplicates_count = querier.duplicates_found
                print("\n=== Deduplication Statistics ===")
                print(f"Duplicates found and skipped: {final_duplicates_count}")
                print(f"Cache size: {cache_stats['size']}/{cache_stats['max_size']}")
                print(f"Cache hit rate: {cache_stats['hit_rate']:.2%}")
                print(f"Total hash checks: {cache_stats['total_checks']}")
            else:
                print("\n=== Deduplication was disabled ===")
                print("All entries were written without duplicate checking")
            
            print(f"\nFiles created: {len(file_writer.get_files_created())}")
            for file_path in file_writer.get_files_created():
                print(f"  - {file_path}")
                
        except Exception as e:
            print(f"[Main] Error during processing: {e}")
            # Cancel all tasks
            producer_task.cancel()
            for task in consumer_tasks:
                task.cancel()
            raise
            
        finally:
            # Ensure file is properly closed
            file_writer.close()
    
    return 0

if __name__ == '__main__':
    print('Starting ...')
    start_time = datetime.now()
    
    try:
        exit_code = asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")
        exit_code = 1
    
    duration = datetime.now() - start_time
    print(f'Completed in: {duration}')
    exit(exit_code)
