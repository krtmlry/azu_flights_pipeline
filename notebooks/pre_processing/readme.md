# Pre-processing

Before running the pipeline it is much easier to download the files locally and pre-process each file and load it into a bucket in the future. 

The pre-processing will only have two goals:
1. Rename columns from Portuguese to English
2. Convert and combine files into one CSV or into one parquet file for each year
3. Set data type for each column to string for easier processing

The raw data will still be intact and will not be changed.

Downloading directly from the source link is doable but it would require a fast internet connection for the process to be efficient.