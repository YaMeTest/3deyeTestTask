using FileSorter;

using var sorter = new LargeFileSorter(
    @"test.txt",
    @"output.txt",
    @"temp");

await sorter.SortFileAsync();