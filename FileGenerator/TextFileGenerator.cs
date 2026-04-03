using System.Buffers;

namespace FileGenerator;

public static class TextFileGenerator
{
    private static ReadOnlySpan<byte> Alphabet =>
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"u8;

    public static void GenerateFile(
        string outputPath,
        long targetSizeBytes,
        int textLengthPerLine = 10,
        int duplicatePoolSize = 100_000,
        int writeBufferSize = 256 * 1024 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(targetSizeBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(textLengthPerLine);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(duplicatePoolSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(writeBufferSize);

        if (targetSizeBytes == 0)
        {
            using var emptyFs = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
            return;
        }

        byte nl1 = (byte)'\n';
        byte nl2 = 0;
        int newlineLength = 1;

        if (OperatingSystem.IsWindows())
        {
            nl1 = (byte)'\r';
            nl2 = (byte)'\n';
            newlineLength = 2;
        }

        var fsOptions = new FileStreamOptions
        {
            Mode = FileMode.Create,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = writeBufferSize,
            Options = FileOptions.SequentialScan,
            PreallocationSize = targetSizeBytes
        };

        using var fs = new FileStream(outputPath, fsOptions);

        byte[] writeBuffer = ArrayPool<byte>.Shared.Rent(writeBufferSize);
        byte[] textPoolBacking = ArrayPool<byte>.Shared.Rent(duplicatePoolSize * textLengthPerLine);

        try
        {
            var random = Random.Shared;
            int bufferPos = 0;
            long written = 0;

            int poolCount = 0;
            int poolWriteIndex = 0;

            Span<byte> numberBuffer = new byte[5];
            Span<byte> tempText = new byte[Math.Min(textLengthPerLine, 1024)];

            if (textLengthPerLine > 1024)
                throw new ArgumentOutOfRangeException(nameof(textLengthPerLine), "Values above 1024 are not supported by this implementation.");

            while (written < targetSizeBytes)
            {
                int number = random.Next(1, 100000);

                bool useDuplicate = poolCount > 0 && random.NextDouble() < 0.1;
                ReadOnlySpan<byte> text;

                if (useDuplicate)
                {
                    int pick = random.Next(poolCount);
                    text = textPoolBacking.AsSpan(pick * textLengthPerLine, textLengthPerLine);
                }
                else
                {
                    FillRandomText(tempText[..textLengthPerLine], random);
                    StoreInPool(tempText[..textLengthPerLine], textPoolBacking, textLengthPerLine, duplicatePoolSize, ref poolCount, ref poolWriteIndex);
                    text = tempText[..textLengthPerLine];
                }

                int numberLen = WritePositiveInt(number, numberBuffer);
                int lineLen = numberLen + 1 + textLengthPerLine + newlineLength;

                if (written + lineLen > targetSizeBytes)
                    break;

                if (bufferPos + lineLen > writeBuffer.Length)
                {
                    fs.Write(writeBuffer, 0, bufferPos);
                    bufferPos = 0;
                }

                numberBuffer[..numberLen].CopyTo(writeBuffer.AsSpan(bufferPos));
                bufferPos += numberLen;

                writeBuffer[bufferPos++] = (byte)'.';

                text.CopyTo(writeBuffer.AsSpan(bufferPos));
                bufferPos += textLengthPerLine;

                writeBuffer[bufferPos++] = nl1;
                if (newlineLength == 2)
                    writeBuffer[bufferPos++] = nl2;

                written += lineLen;
            }

            if (bufferPos > 0)
                fs.Write(writeBuffer, 0, bufferPos);

            fs.Flush();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(writeBuffer);
            ArrayPool<byte>.Shared.Return(textPoolBacking);
        }
    }

    private static void FillRandomText(Span<byte> destination, Random random)
    {
        ReadOnlySpan<byte> alphabet = Alphabet;
        int alphabetLength = alphabet.Length;

        for (int i = 0; i < destination.Length; i++)
            destination[i] = alphabet[random.Next(alphabetLength)];
    }

    private static void StoreInPool(
        ReadOnlySpan<byte> text,
        byte[] backingStore,
        int textLength,
        int poolCapacity,
        ref int poolCount,
        ref int poolWriteIndex)
    {
        int offset = poolWriteIndex * textLength;
        text.CopyTo(backingStore.AsSpan(offset, textLength));

        if (poolCount < poolCapacity)
            poolCount++;

        poolWriteIndex++;
        if (poolWriteIndex == poolCapacity)
            poolWriteIndex = 0;
    }

    private static int WritePositiveInt(int value, Span<byte> destination)
    {
        int pos = destination.Length;

        do
        {
            int digit = value % 10;
            value /= 10;
            destination[--pos] = (byte)('0' + digit);
        }
        while (value != 0);

        int len = destination.Length - pos;
        destination[pos..].CopyTo(destination);
        return len;
    }
}