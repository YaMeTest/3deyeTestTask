# 3deyeTestTask

A .NET solution for generating and sorting very large text files.

The repository contains two console applications:

- **FileGenerator** — creates a large test file with lines in the format:
  `number.randomText`
- **FileSorter** — sorts that file using an external sort approach designed for files too large to fit in memory

## Repository structure

```text
3deyeTestTask/
├─ FileGenerator/
│  ├─ Program.cs
│  ├─ TextFileGenerator.cs
│  └─ FileGenerator.csproj
├─ FileSorter/
│  ├─ Program.cs
│  ├─ LargeFileSorter.cs
│  ├─ LineRecord.cs
│  ├─ ChunkInfo.cs
│  ├─ SortJobState.cs
│  ├─ SortJobStatus.cs
│  └─ FileSorter.csproj
└─ EffectiveFileSorter.slnx
```

Requirements
.NET 10.0 SDK

Both projects target net10.0.

Input format

The sorter expects each input line to follow this pattern:

```<number>.<text>```

Example:

415.apple
7.orange
415.banana
Sorting rules

Records are sorted by:

Text part (ascending, ordinal comparison)
Number part (ascending) when the text part is equal

So this input:

10.apple
2.apple
8.banana
1.carrot

becomes:

2.apple
10.apple
8.banana
1.carrot
Project 1: FileGenerator

FileGenerator is a console app that generates a text file for testing.

Current behavior

In Program.cs, it generates a file named test.txt with a target size of approximately 1000 × 1024 × 1024 bytes and uses a text length of 12 characters per line.

Generation details

The generator:

writes lines as number.text
uses random numbers in the range 1..99999
uses alphanumeric random text
intentionally reuses previously generated text values with a probability of about 10%
uses buffered writing and file preallocation for large output files
Run
dotnet run --project FileGenerator

After completion, a file named test.txt is created in the working directory.

Project 2: FileSorter

FileSorter is a console app that sorts a large file using an external merge sort strategy.

Current behavior

In Program.cs, it runs:

input file: test.txt
output file: output.txt
temp directory: temp
How it works

The sorter processes the input in several phases:

1. Chunking

The input file is read line by line. Parsed records are accumulated until an estimated chunk size limit is reached.

2. In-memory sort

Each chunk is sorted in memory using the LineRecordComparer.

3. Chunk write

Each sorted chunk is written to a temporary chunk file.

4. Multi-pass merge

Chunk files are merged in batches until only one final sorted file remains.

5. Final output

The final merged file is copied to the requested output path.

Resumable state

The sorter persists progress in a JSON state file:

sort-state.json

The saved state includes:

input path
output path
working directory
creation/completion timestamps
current status
number of consumed lines
completed chunks
completed merge batches

This allows the process structure to support restart/resume scenarios.

Important implementation settings

LargeFileSorter currently uses these defaults:

maxChunkBytes = 1024 * 1024 * 1024
mergeFileCount = 32
maxConcurrentChunkJobs = 6

The file stream configuration also uses large buffers and asynchronous I/O.

Run
dotnet run --project FileSorter

This reads test.txt, sorts it, and writes the result to output.txt.

Example workflow
1. Generate the test file
dotnet run --project FileGenerator
2. Sort the file
dotnet run --project FileSorter
3. Result

You should end up with:

test.txt — generated input file
output.txt — sorted output file
temp/ — temporary working files used during sorting
Notes
The solution is built as two separate console apps rather than a single CLI.
The sorter is designed for very large files and avoids loading the full input into memory at once.
The comparison logic is based on the text part first, then the numeric part.
The generator and sorter currently use hardcoded paths in Program.cs.
Possible improvements
Add command-line arguments for input/output/temp paths
Add configurable chunk size and merge fan-out
Add logging and progress reporting
Add validation and benchmark results
Add automated tests for parsing, comparison, chunking, and merging
Add cleanup rules for temporary files after successful completion
Add cancellation and retry handling at the application level
