using FileSorter;

var sorter = new LargeFileSorter(
    @"C:\Users\YaraslauMiatselitsa\source\repos\FileGenerator\bin\Debug\net10.0\test.txt",
    @"output.txt",
    @"temp");

await sorter.SortFileAsync();