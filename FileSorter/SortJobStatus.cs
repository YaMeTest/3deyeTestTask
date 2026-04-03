namespace FileSorter;

public enum SortJobStatus
{
    Created = 0,
    Chunking = 1,
    Merging = 2,
    Completed = 3
}
