#!/usr/bin/env python3
import pandas as pd
import os
import math

def split_parquet_file(input_file, max_size_gb=1.8):
    """
    Split a large Parquet file into smaller chunks.
    
    Args:
        input_file: Path to the input Parquet file
        max_size_gb: Maximum size in GB for each chunk (default 1.8GB to be safe)
    """
    print(f"Splitting {input_file}...")
    
    # Read the Parquet file
    df = pd.read_parquet(input_file)
    total_rows = len(df)
    
    # Calculate chunk size based on max_size_gb
    # Estimate rows per GB (rough approximation)
    file_size_gb = os.path.getsize(input_file) / (1024**3)
    rows_per_gb = total_rows / file_size_gb
    target_rows_per_chunk = int(rows_per_gb * max_size_gb)
    
    # Ensure we don't exceed total rows
    target_rows_per_chunk = min(target_rows_per_chunk, total_rows)
    
    # Calculate number of chunks
    num_chunks = math.ceil(total_rows / target_rows_per_chunk)
    
    print(f"Total rows: {total_rows}")
    print(f"Target rows per chunk: {target_rows_per_chunk}")
    print(f"Number of chunks: {num_chunks}")
    
    # Split and save chunks
    base_name = os.path.splitext(input_file)[0]
    
    for i in range(num_chunks):
        start_idx = i * target_rows_per_chunk
        end_idx = min((i + 1) * target_rows_per_chunk, total_rows)
        
        chunk = df.iloc[start_idx:end_idx]
        output_file = f"{base_name}_part_{i+1:03d}.parquet"
        
        print(f"Saving chunk {i+1}/{num_chunks} to {output_file} ({len(chunk)} rows)")
        chunk.to_parquet(output_file, index=False)
        
        # Verify file size
        chunk_size_gb = os.path.getsize(output_file) / (1024**3)
        print(f"  Chunk size: {chunk_size_gb:.2f} GB")
    
    print(f"Split complete! Created {num_chunks} files.")

if __name__ == "__main__":
    # Split both large files
    files_to_split = ["train.parquet", "test.parquet"]
    
    for file in files_to_split:
        if os.path.exists(file):
            split_parquet_file(file)
        else:
            print(f"File {file} not found, skipping...") 