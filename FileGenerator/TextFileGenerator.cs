using System.Text;

namespace FileGenerator;

public static class TextFileGenerator
{
    private static readonly char[] Alphabet =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

    public static void GenerateFile(
        string outputPath,
        long targetSizeBytes,
        int textLengthPerLine = 10,
        int batchLines = 10000,
        double duplicateProbability = 0.1)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(targetSizeBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(textLengthPerLine);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchLines);

        if (duplicateProbability < 0 || duplicateProbability > 1)
            throw new ArgumentOutOfRangeException(nameof(duplicateProbability), "Value must be between 0 and 1.");

        var encoding = new UTF8Encoding(false);
        string newLine = Environment.NewLine;
        var random = new Random();

        long writtenBytes = 0;

        using var fs = new FileStream(
            outputPath,
            FileMode.Create,
            FileAccess.Write,
            FileShare.None,
            4 * 1024 * 1024,
            FileOptions.WriteThrough);

        using var writer = new StreamWriter(fs, encoding, 4 * 1024 * 1024);

        var sb = new StringBuilder(batchLines * (textLengthPerLine + 4));
        var stringPool = new List<string>(1024);

        while (writtenBytes < targetSizeBytes)
        {
            sb.Clear();
            var textPart = GenerateRandomString(textLengthPerLine, random);
            stringPool.Add(textPart);

            for (int i = 0; i < batchLines; i++)
            {
                bool useDuplicate = random.NextDouble() < duplicateProbability;

                if (useDuplicate)
                {
                    textPart = stringPool[random.Next(stringPool.Count)];
                }
                else
                {
                    textPart = GenerateRandomString(textLengthPerLine, random);
                    stringPool.Add(textPart);
                }

                string line = $"{random.Next(1, 100000)}.{textPart}{newLine}";
                int lineSize = encoding.GetByteCount(line);

                if (writtenBytes + lineSize > targetSizeBytes)
                {
                    writer.Write(sb.ToString());
                    writer.Flush();
                    return;
                }

                sb.Append(line);
                writtenBytes += lineSize;
            }

            writer.Write(sb.ToString());
        }

        writer.Flush();
    }

    private static string GenerateRandomString(int length, Random random)
    {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = Alphabet[random.Next(Alphabet.Length)];

        return new string(chars);
    }
}