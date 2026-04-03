using System.Globalization;
using System.Text.Json;

namespace FileSorter;

public sealed class LargeFileSorter : IDisposable
{
    private readonly long _maxChunkBytes;
    private readonly int _mergeFileCount;
    private readonly int _maxConcurrentChunkJobs;

    private readonly string _jobId;
    private readonly string _inputPath;
    private readonly string _outputPath;
    private readonly string _tempDirectory;
    private readonly string _workingDirectory;

    private readonly string _statePath;

    private const string StateFileName = "sort-state.json";


    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private static readonly FileStreamOptions ReadOptions = new()
    {
        Access = FileAccess.Read,
        Mode = FileMode.Open,
        Share = FileShare.Read,
        BufferSize = 256 * 1024 * 1024,
        Options = FileOptions.SequentialScan | FileOptions.Asynchronous
    };

    private static readonly FileStreamOptions WriteOptions = new()
    {
        Access = FileAccess.Write,
        Mode = FileMode.Create,
        Share = FileShare.None,
        BufferSize = 256 * 1024 * 1024,
        Options = FileOptions.Asynchronous
    };

    public LargeFileSorter(
        string inputPath,
        string outputPath,
        string tempDirectory,
        long maxChunkBytes = 1024 * 1024 * 1024,
        int mergeFileCount = 32,
        int maxConcurrentChunkJobs = 6)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxChunkBytes);
        ArgumentOutOfRangeException.ThrowIfLessThan(mergeFileCount, 2);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrentChunkJobs, 1);
        ArgumentException.ThrowIfNullOrWhiteSpace(inputPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        ArgumentException.ThrowIfNullOrWhiteSpace(tempDirectory);

        _inputPath = inputPath;
        _outputPath = outputPath;
        _tempDirectory = tempDirectory;

        _mergeFileCount = mergeFileCount;
        _maxChunkBytes = maxChunkBytes;
        _maxConcurrentChunkJobs = maxConcurrentChunkJobs;

        _jobId = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(Path.GetFullPath(_inputPath)))).ToLowerInvariant()[..16];

        Directory.CreateDirectory(_tempDirectory);
        _workingDirectory = Path.Combine(_tempDirectory, $"sort-{_jobId}");
        Directory.CreateDirectory(_workingDirectory);

        _statePath = Path.Combine(_workingDirectory, StateFileName);
    }

    public async Task SortFileAsync(CancellationToken cancellationToken = default)
    {
        if (!File.Exists(_inputPath))
            throw new FileNotFoundException("Input file not found.", _inputPath);

        var state = await LoadOrCreateStateAsync(cancellationToken).ConfigureAwait(false);

        ValidateStateAgainstRequest(state);

        await CreateSortedChunksAsync(state, cancellationToken).ConfigureAwait(false);
        await MergeSortedChunksMultiPassAsync(state, cancellationToken).ConfigureAwait(false);

        state.Status = SortJobStatus.Completed;
        state.CompletedAtUtc = DateTimeOffset.UtcNow;
        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
    }

    private async Task CreateSortedChunksAsync(SortJobState state, CancellationToken cancellationToken)
    {
        if (state.ChunkingCompleted)
            return;

        state.Status = SortJobStatus.Chunking;
        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);

        using var fs = new FileStream(_inputPath, ReadOptions);
        using var reader = new StreamReader(fs);

        var semaphore = new SemaphoreSlim(_maxConcurrentChunkJobs);
        var pending = new List<Task<ChunkWriteResult>>();
        var chunk = new List<LineRecord>(2048);
        long estimatedBytes = 0;
        long lineNumber = 0;
        int nextChunkIndex = state.CompletedChunks.Count;

        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false)) is not null)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lineNumber++;

            if (lineNumber <= state.LinesConsumed)
                continue;

            if (!TryParseLine(line, out var record))
                throw new FormatException($"Invalid input line: '{line}'");

            chunk.Add(record);
            estimatedBytes += (line.Length * sizeof(char)) + 96;

            if (estimatedBytes >= _maxChunkBytes)
            {
                int chunkIndex = nextChunkIndex++;
                pending.Add(ScheduleChunkWriteAsync(chunk, semaphore, chunkIndex, lineNumber, cancellationToken));

                chunk = new List<LineRecord>(2048);
                estimatedBytes = 0;
            }
        }

        if (chunk.Count > 0)
        {
            int chunkIndex = nextChunkIndex++;
            pending.Add(ScheduleChunkWriteAsync(chunk, semaphore, chunkIndex, lineNumber, cancellationToken));
        }

        while (pending.Count > 0)
        {
            Task<ChunkWriteResult> finished = await Task.WhenAny(pending).ConfigureAwait(false);
            pending.Remove(finished);

            ChunkWriteResult result = await finished.ConfigureAwait(false);

            state.CompletedChunks.Add(new ChunkInfo
            {
                ChunkIndex = result.ChunkIndex,
                Path = result.Path,
                EndLineNumber = result.EndLineNumber
            });

            state.CompletedChunks.Sort((a, b) => a.ChunkIndex.CompareTo(b.ChunkIndex));
            state.LinesConsumed = Math.Max(state.LinesConsumed, result.EndLineNumber);

            await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
        }

        state.ChunkingCompleted = true;
        state.Status = SortJobStatus.Merging;
        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
    }

    private Task<ChunkWriteResult> ScheduleChunkWriteAsync(
        List<LineRecord> chunk,
        SemaphoreSlim semaphore,
        int chunkIndex,
        long endLineNumber,
        CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                string path = await WriteSortedChunkAsync(chunk, chunkIndex, cancellationToken).ConfigureAwait(false);

                return new ChunkWriteResult(chunkIndex, path, endLineNumber);
            }
            finally
            {
                semaphore.Release();
            }
        }, cancellationToken);
    }

    private async Task<string> WriteSortedChunkAsync(List<LineRecord> chunk, int chunkIndex, CancellationToken cancellationToken)
    {
        chunk.Sort(LineRecordComparer.Instance);

        string tempFile = Path.Combine(_workingDirectory, $"chunk-{chunkIndex:D8}.chunk");
        string partialFile = tempFile + ".partial";

        if (File.Exists(tempFile))
            return tempFile;

        if (File.Exists(partialFile))
            File.Delete(partialFile);

        using (var fs = new FileStream(partialFile, WriteOptions))
        using (var writer = new StreamWriter(fs))
        {
            foreach (var record in chunk)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.WriteLineAsync(record.OriginalLine).ConfigureAwait(false);
            }

            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        File.Move(partialFile, tempFile, overwrite: true);
        return tempFile;
    }

    private async Task MergeSortedChunksMultiPassAsync(SortJobState state, CancellationToken cancellationToken)
    {
        var current = state.CompletedChunks
            .OrderBy(x => x.ChunkIndex)
            .Select(x => x.Path)
            .ToArray();

        if (current.Length == 0)
        {
            string empty = Path.Combine(_workingDirectory, "empty.out");
            if (!File.Exists(empty))
                File.Create(empty).Dispose();

            state.MergeCompleted = true;
            state.FinalFilePath = empty;
            await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
        }

        if (current.Length == 1)
        {
            state.MergeCompleted = true;
            state.FinalFilePath = current[0];
            await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
        }

        int passNumber = 0;

        while (current.Length > 1)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var next = new List<string>((current.Length + _mergeFileCount - 1) / _mergeFileCount);

            for (int i = 0; i < current.Length; i += _mergeFileCount)
            {
                var batch = current.Skip(i).Take(_mergeFileCount).ToArray();
                int batchNumber = i / _mergeFileCount;
                string merged = Path.Combine(_workingDirectory, $"merge-p{passNumber:D4}-b{batchNumber:D8}.merge");

                if (!File.Exists(merged))
                {
                    await MergeBatchAsync(batch, merged, cancellationToken).ConfigureAwait(false);
                }

                next.Add(merged);

                string key = $"p{passNumber:D4}-b{batchNumber:D8}";
                if (state.CompletedMergeBatches.Add(key))
                {
                    await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
                }

                foreach (var filePath in batch)
                {
                    if (string.Equals(filePath, merged, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (Path.GetFileName(filePath).StartsWith("chunk-", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (File.Exists(filePath))
                        File.Delete(filePath);
                }
            }

            current = [.. next];
            passNumber++;
        }

        File.Copy(current[0], _outputPath, overwrite: true);

        state.MergeCompleted = true;
        state.FinalFilePath = _outputPath;
        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
    }

    private static async Task MergeBatchAsync(
        string[] batchFiles,
        string mergePath,
        CancellationToken cancellationToken)
    {
        string partialOutput = mergePath + ".partial";

        if (File.Exists(mergePath))
            return;

        if (File.Exists(partialOutput))
            File.Delete(partialOutput);

        var readers = new List<StreamReader>(batchFiles.Length);

        try
        {
            var pq = new PriorityQueue<MergeItem, LineRecord>(LineRecordComparer.Instance);

            foreach (string file in batchFiles)
            {
                var fs = new FileStream(file, ReadOptions);
                var reader = new StreamReader(fs);
                readers.Add(reader);

                string? line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (line is null)
                    continue;

                if (!TryParseLine(line, out var record))
                    throw new FormatException($"Invalid chunk line: '{line}'");

                pq.Enqueue(new MergeItem(reader, record.OriginalLine), record);
            }

            using (var outFs = new FileStream(partialOutput, WriteOptions))
            using (var writer = new StreamWriter(outFs))
            {
                while (pq.Count > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    MergeItem item = pq.Dequeue();
                    await writer.WriteLineAsync(item.Line).ConfigureAwait(false);

                    string? nextLine = await item.Reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                    if (nextLine is null)
                        continue;

                    if (!TryParseLine(nextLine, out var nextRecord))
                        throw new FormatException($"Invalid chunk line: '{nextLine}'");

                    pq.Enqueue(new MergeItem(item.Reader, nextRecord.OriginalLine), nextRecord);
                }

                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            File.Move(partialOutput, mergePath, overwrite: true);
        }
        finally
        {
            foreach (var reader in readers)
            {
                reader.Dispose();
            }
        }
    }

    private async Task<SortJobState> LoadOrCreateStateAsync(CancellationToken cancellationToken)
    {
        if (File.Exists(_statePath))
        {
            await using var readStream = File.OpenRead(_statePath);
            var loaded = await JsonSerializer.DeserializeAsync<SortJobState>(readStream, JsonOptions, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException("State file exists but could not be deserialized.");

            loaded.WorkingDirectory = _workingDirectory;
            loaded.OutputPath = _outputPath;
            return loaded;
        }

        var created = new SortJobState
        {
            InputPath = Path.GetFullPath(_inputPath),
            OutputPath = Path.GetFullPath(_outputPath),
            WorkingDirectory = Path.GetFullPath(_workingDirectory),
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Status = SortJobStatus.Created
        };

        File.Create(_statePath).Close();

        await SaveStateAsync(created, cancellationToken).ConfigureAwait(false);
        return created;
    }

    private async Task SaveStateAsync(SortJobState state, CancellationToken cancellationToken)
    {
        await using var fs = new FileStream(_statePath, WriteOptions);
        await JsonSerializer.SerializeAsync(fs, state, JsonOptions, cancellationToken).ConfigureAwait(false);
        await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private void ValidateStateAgainstRequest(SortJobState state)
    {
        string inputFull = Path.GetFullPath(_inputPath);
        string outputFull = Path.GetFullPath(_outputPath);

        if (!string.Equals(state.InputPath, inputFull, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Existing state belongs to a different input file.");

        if (!string.Equals(state.OutputPath, outputFull, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Existing state belongs to a different output file.");
    }

    private static bool TryParseLine(string line, out LineRecord record)
    {
        int dotIndex = line.IndexOf('.');
        if (dotIndex <= 0)
        {
            record = default;
            return false;
        }

        ReadOnlySpan<char> numberPart = line.AsSpan(0, dotIndex).Trim();
        if (!long.TryParse(numberPart, NumberStyles.None, CultureInfo.InvariantCulture, out long number))
        {
            record = default;
            return false;
        }

        int textStart = dotIndex + 1;
        while (textStart < line.Length && char.IsWhiteSpace(line[textStart]))
        {
            textStart++;
        }

        record = new LineRecord(number, line, textStart);
        return true;
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDirectory))
            Directory.Delete(_tempDirectory, recursive: true);
    }

    private readonly record struct MergeItem(StreamReader Reader, string Line);
    private readonly record struct ChunkWriteResult(int ChunkIndex, string Path, long EndLineNumber);
}
