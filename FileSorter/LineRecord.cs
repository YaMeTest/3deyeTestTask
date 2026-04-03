namespace FileSorter;

public readonly record struct LineRecord(long Number, string OriginalLine, int TextStart)
{
    public ReadOnlySpan<char> TextSpan => OriginalLine.AsSpan(TextStart);
}

public sealed class LineRecordComparer : IComparer<LineRecord>
{
    public static readonly LineRecordComparer Instance = new();

    public int Compare(LineRecord x, LineRecord y)
    {
        int textCompare = x.TextSpan.CompareTo(y.TextSpan, StringComparison.Ordinal);
        if (textCompare != 0)
            return textCompare;

        int numberCompare = x.Number.CompareTo(y.Number);
        if (numberCompare != 0)
            return numberCompare;

        return 0;
    }
}
