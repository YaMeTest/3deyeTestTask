namespace FileSorter;

public sealed class SortJobState
{
    public string InputPath { get; set; } = "";
    public string OutputPath { get; set; } = "";
    public string WorkingDirectory { get; set; } = "";
    public DateTimeOffset CreatedAtUtc { get; set; }
    public DateTimeOffset? CompletedAtUtc { get; set; }
    public SortJobStatus Status { get; set; }
    public long LinesConsumed { get; set; }
    public bool ChunkingCompleted { get; set; }
    public bool MergeCompleted { get; set; }
    public string? FinalFilePath { get; set; }
    public List<ChunkInfo> CompletedChunks { get; set; } = [];
    public HashSet<string> CompletedMergeBatches { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}