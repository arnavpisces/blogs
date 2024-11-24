from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import mmap
import os
import time
from datetime import datetime

def get_chunk_boundaries(mm, num_chunks):
    file_size = len(mm)
    approximate_chunk_size = file_size // num_chunks
    boundaries = []
    current_pos = 0
    for i in range(num_chunks):
        chunk_start = current_pos
        # If this is the last chunk, set end to file size
        if i == num_chunks - 1:
            boundaries.append((chunk_start, file_size))
            break
        # Find the next newline after the approximate chunk size
        next_pos = min(chunk_start + approximate_chunk_size, file_size)
        # Scan forward to find the next newline
        while next_pos < file_size and mm[next_pos:next_pos+1] != b'\n':
            next_pos += 1
        # Add 1 to include the newline in the current chunk
        next_pos += 1
        boundaries.append((chunk_start, next_pos))
        current_pos = next_pos
        
    return boundaries

def process_chunk_threaded(mm, start, end, chunk_id):
    print(f"[{datetime.now()}] Starting chunk {chunk_id}, processing {(end-start)/(1024):.2f} KB")
    start_time = time.time()
    results = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0})
    # Read and process the chunk
    chunk = mm[start:end].decode('utf-8')
    for line in chunk.split('\n'):
        if not line or line.startswith('#'):  # Skip empty lines and comments
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

def process_file(filename, num_threads=os.cpu_count()):
    print(f"\n[{datetime.now()}] Starting processing with {num_threads} threads")
    start_time = time.time()
    
    # Open file with memory mapping
    with open(filename, 'rb') as f:
        file_size = os.path.getsize(filename)
        print(f"[{datetime.now()}] File size: {file_size/(1024):.2f} KB")
        
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        # Calculate chunk boundaries
        boundaries = get_chunk_boundaries(mm, num_threads)
        
        # Print chunk sizes for verification
        for i, (start, end) in enumerate(boundaries):
            chunk_size = end - start
            print(f"[{datetime.now()}] Chunk {i} size: {chunk_size/(1024):.2f} KB")
        
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(process_chunk_threaded, mm, start, end, i) 
                      for i, (start, end) in enumerate(boundaries)]
            
            # Merge results as they complete
            print(f"[{datetime.now()}] Processing chunks...")
            chunk_results = []
            for future in futures:
                try:
                    result = future.result()
                    chunk_results.append(result)
                except Exception as e:
                    print(f"[{datetime.now()}] Error processing chunk: {str(e)}")
        
        # Merge results
        print(f"[{datetime.now()}] Merging results from all chunks...")
        final_results = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'sum': 0, 'count': 0})
        for chunk_result in chunk_results:
            for station, stats in chunk_result.items():
                final_results[station]['min'] = min(final_results[station]['min'], stats['min'])
                final_results[station]['max'] = max(final_results[station]['max'], stats['max'])
                final_results[station]['sum'] += stats['sum']
                final_results[station]['count'] += stats['count']
        
        mm.close()
        total_time = time.time() - start_time
        print(f"[{datetime.now()}] Total processing time: {total_time:.4f} seconds")
        print(f"[{datetime.now()}] Processing speed: {(file_size/(1024*1024))/total_time:.2f} MB/second")
        
        return dict(final_results)

if __name__ == '__main__':
    print(f"[{datetime.now()}] Starting weather station data processing...")
    results = process_file('./weather_stations.csv')
    
    # Print results
    print(f"\n[{datetime.now()}] Results:")
    print("-" * 60)
    # for station, stats in results.items():
        # avg = stats['sum'] / stats['count']
        # print(f"{station}: min={stats['min']:.1f}, max={stats['max']:.1f}, avg={avg:.1f}")
    print("-" * 60)