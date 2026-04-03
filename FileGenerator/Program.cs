using FileGenerator;

long sizeBytes = 1000 * 1024 * 1024;

TextFileGenerator.GenerateFile(
    outputPath: "test.txt",
    targetSizeBytes: sizeBytes,
    textLengthPerLine: 12
);

Console.WriteLine("File generated.");