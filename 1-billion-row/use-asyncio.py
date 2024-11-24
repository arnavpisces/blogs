import asyncio
from collections import defaultdict
import mmap
import os
import time
from datetime import datetime

async def get_file_size(filename):
    return os.path.getsize(filename)

async def open_mmap(filename):
    loop = asyncio.get_event_loop()
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

async def process_chunk(mm, start, end, chunk_id):
    print(f"[{datetime.now()}] Starting chunk {chunk_id}, processing {(end-start)/(1024):.2f} KB")
    start_time = time.time()
    results = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0})
    chunk = mm[start:end].decode('utf-8')
    
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

async def process_file(filename, num_chunks=os.cpu_count()):
    print(f"\n[{datetime.now()}] Starting processing with {num_chunks} chunks")
    start_time = time.time()
    
    mm = await open_mmap(filename)
    boundaries = calculate_boundaries(mm, num_chunks)
    
    tasks = [process_chunk(mm, start, end, i) for i, (start, end) in enumerate(boundaries)]
    chunk_results = await asyncio.gather(*tasks)
    
    final_results = merge_results(chunk_results)
    mm.close()
    total_time = time.time() - start_time
    print(f"[{datetime.now()}] Total processing time: {total_time:.4f} seconds")
    file_size = await get_file_size(filename)
    processing_speed = (file_size / (1024 * 1024)) / total_time  # MB/second
    print(f"[{datetime.now()}] Processing speed: {processing_speed:.2f} MB/second")
    
    return final_results

if __name__ == '__main__':
    print(f"[{datetime.now()}] Starting weather station data processing...")
    results = asyncio.run(process_file('./weather_stations.csv'))
    
    print(f"\n[{datetime.now()}] Results:")
    print("-" * 60)
    # for station, stats in results.items():
        # avg = stats['sum'] / stats['count']
        # print(f"{station}: min={stats['min']:.1f}, max={stats['max']:.1f}, avg={avg:.1f}")
    print("-" * 60)