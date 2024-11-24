from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import mmap
import os
import time
from datetime import datetime

def get_file_size(filename):
    return os.path.getsize(filename)

def open_mmap(filename):
    with open(filename, 'rb') as f:
        return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

def calculate_boundaries(mm, num_chunks):
    file_size = len(mm)
    approximate_chunk_size = file_size // num_chunks
    boundaries = []
    current_pos = 0
    for i in range(num_chunks):
        chunk_start = current_pos
        if i == num_chunks - 1:
            boundaries.append((chunk_start, file_size))
            break
        next_pos = min(chunk_start + approximate_chunk_size, file_size)
        while next_pos < file_size and mm[next_pos:next_pos+1] != b'\n':
            next_pos += 1
        next_pos += 1
        boundaries.append((chunk_start, next_pos))
        current_pos = next_pos
    return boundaries

def process_chunk(filename, start, end, chunk_id):
    print(f"[{datetime.now()}] Starting chunk {chunk_id}, processing {(end-start)/(1024):.2f} KB")
    start_time = time.time()
    results = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0})
    
    # Open mmap within the function to avoid pickling issues
    mm = open_mmap(filename)
    chunk = mm[start:end].decode('utf-8')
    mm.close()  # Close mmap after reading the chunk
    
    for line in chunk.split('\n'):
        if not line or line.startswith('#'):
            continue
        try:
            station, temp = line.split(';')
            temp = float(temp)
            results[station]['min'] = min(results[station]['min'], temp)
            results[station]['max'] = max(results[station]['max'], temp)
            results[station]['sum'] += temp
            results[station]['count'] += 1
        except (ValueError, IndexError) as e:
            print(f"[{datetime.now()}] Warning: Skipping malformed line in chunk {chunk_id}")
            continue
    processing_time = time.time() - start_time
    print(f"[{datetime.now()}] Finished chunk {chunk_id} in {processing_time:.4f} seconds")
    return dict(results)

def merge_results(chunk_results):
    final_results = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0})
    for chunk_result in chunk_results:
        for station, stats in chunk_result.items():
            final_results[station]['min'] = min(final_results[station]['min'], stats['min'])
            final_results[station]['max'] = max(final_results[station]['max'], stats['max'])
            final_results[station]['sum'] += stats['sum']
            final_results[station]['count'] += stats['count']
    return dict(final_results)

def process_file(filename, num_processes=os.cpu_count()):
    print(f"\n[{datetime.now()}] Starting processing with {num_processes} processes")
    start_time = time.time()
    
    boundaries = calculate_boundaries(open_mmap(filename), num_processes)  # Open mmap only for boundaries
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(process_chunk, filename, start, end, i) 
                  for i, (start, end) in enumerate(boundaries)]
        
        print(f"[{datetime.now()}] Processing chunks...")
        chunk_results = [future.result() for future in futures]
    
    final_results = merge_results(chunk_results)
    total_time = time.time() - start_time
    print(f"[{datetime.now()}] Total processing time: {total_time:.4f} seconds")
    file_size = get_file_size(filename)
    processing_speed = (file_size / (1024 * 1024)) / total_time  # MB/second
    print(f"[{datetime.now()}] Processing speed: {processing_speed:.2f} MB/second")
    return final_results

if __name__ == '__main__':
    print(f"[{datetime.now()}] Starting weather station data processing...")
    results = process_file('./weather_stations.csv')
    
    print(f"\n[{datetime.now()}] Results:")
    print("-" * 60)
    # for station, stats in results.items():
        # avg = stats['sum'] / stats['count']
        # print(f"{station}: min={stats['min']:.1f}, max={stats['max']:.1f}, avg={avg:.1f}")
    print("-" * 60)