// See https://aka.ms/new-console-template for more information

using S3RowsCounter;
using Spectre.Console;

AnsiConsole.WriteLine("Please enter bucket name:-");
string bucketName = Console.ReadLine() ?? string.Empty;

AnsiConsole.WriteLine("Please enter folder name:-");
string folder = Console.ReadLine() ?? string.Empty;

AnsiConsole.WriteLine();

await CountTotalRowsScript.Run(bucketName, folder);