# DRW Data Repository

This repository contains large Parquet files that have been split into smaller chunks to comply with GitHub's file size limits.

## File Structure

The original large files have been split into smaller chunks:

- `train.parquet` → `train_part_001.parquet`, `train_part_002.parquet`
- `test.parquet` → `test_part_001.parquet`, `test_part_002.parquet`

## Working with Split Files

### Combining the files back into original format

```python
import pandas as pd
import glob

# Combine train files
train_files = sorted(glob.glob("train_part_*.parquet"))
train_df = pd.concat([pd.read_parquet(f) for f in train_files], ignore_index=True)
train_df.to_parquet("train.parquet", index=False)

# Combine test files
test_files = sorted(glob.glob("test_part_*.parquet"))
test_df = pd.concat([pd.read_parquet(f) for f in test_files], ignore_index=True)
test_df.to_parquet("test.parquet", index=False)
```

### Processing files individually

If you need to process the data in chunks to save memory:

```python
import pandas as pd
import glob

# Process train files one by one
for train_file in sorted(glob.glob("train_part_*.parquet")):
    df = pd.read_parquet(train_file)
    # Process your data here
    print(f"Processing {train_file} with {len(df)} rows")
```

## File Sizes

- `train_part_001.parquet`: ~1.89 GB
- `train_part_002.parquet`: ~1.59 GB
- `test_part_001.parquet`: ~1.90 GB
- `test_part_002.parquet`: ~1.67 GB

All files are tracked with Git LFS and are under the 2GB GitHub limit.

## Notes

- The files were split using the `split_parquet.py` script
- Original file order is preserved within each chunk
- All files use the same schema and data types as the original 