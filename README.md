# Implementation Specs
1. The metadata file should contain information about the size of the different columns (in bytes).
2. The data type for all columns should be a string.
3. The number of columns can range from 1 to 20.
4. Both ascending and descending sorts are possible.
5. The program can run for different values of main memory usage allowed and the different size of files (MBs-GBs).

# Usage

## Basic Version
python3 externalMergeSort.py input.txt output.txt 50 asc C1 C2

## With Threads
python3 externalMergeSort.py input.txt output.txt 100 5 desc C3 C1

# Input File
It should contain the records with the column values. All the values should be string only and might contain space or “,”.

# Command-line inputs:
1. Input file name (containing the raw records)
2. Output filename (containing sorted records)
3. Main memory size (in MB)
4. Number of threads (optional)
5. Order code (asc / desc) asc : ascending, desc : descending
6. ColumnName K1
7. ColumnName K2
8. .....
