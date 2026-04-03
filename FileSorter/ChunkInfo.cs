namespace FileSorter;

public sealed class ChunkInfo
{
    public int ChunkIndex { get; set; }
    public string Path { get; set; } = "";
    public long EndLineNumber { get; set; }
}
