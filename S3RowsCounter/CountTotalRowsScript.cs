using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Spectre.Console;

namespace S3RowsCounter;

public static class CountTotalRowsScript
{
    public static async Task Run(string bucket, string folder)
    {
        string tableName = folder.TrimEnd('/').Split("/").Last();

        var listResponse
            = await AnsiConsole
                    .Status()
                    .StartAsync($"Scanning files for table {tableName}...", async _ =>
                    {
                        AnsiConsole.MarkupLine("Querying S3...");
                        var response = await new AmazonS3Client().ListObjectsAsync(new ListObjectsRequest { BucketName = bucket, Prefix = folder });
                        AnsiConsole.MarkupLine($"Found {response.S3Objects.Count} files.");
                        return response;
                    });

        int totalRowCount
            = await AnsiConsole
                    .Progress()
                    .Columns(new TaskDescriptionColumn(),
                        new ProgressBarColumn(),
                        new CountColumn(),
                        new ElapsedTimeColumn(),
                        new SpinnerColumn())
                    .StartAsync(async ctx =>
                    {
                        int totalCount = 0;
                        var countTasksInProgress = ctx.AddTask($"[green] Counting rows for table {tableName} [/]",
                            true,
                            listResponse.S3Objects.Count);

                        while (!ctx.IsFinished)
                        {
                            foreach (var s3Object in listResponse.S3Objects)
                            {
                                totalCount += await GetRowCountAsync(bucket, s3Object);
                                countTasksInProgress.Increment(1);
                            }
                        }

                        return totalCount;
                    });

        Console.WriteLine($"Total rows in table {tableName}: {totalRowCount}");
    }

    private static async Task<int> GetRowCountAsync(string bucket, S3Object s3Object, bool hasHeaderRow = true)
    {
        var response = await new AmazonS3Client().SelectObjectContentAsync(new SelectObjectContentRequest
        {
            BucketName = bucket,
            Key = s3Object.Key,
            ExpressionType = ExpressionType.SQL,
            Expression = "SELECT COUNT(*) from S3Object",
            InputSerialization = new InputSerialization { CSV = new CSVInput(), CompressionType = CompressionType.Gzip },
            OutputSerialization = new OutputSerialization() { CSV = new CSVOutput() }
        });

        using var eventStream = response.Payload;
        var recordsEvent = eventStream.OfType<RecordsEvent>().First();
        using var reader = new StreamReader(recordsEvent.Payload, Encoding.UTF8);
        int rowCount = int.Parse(await reader.ReadToEndAsync());

        return hasHeaderRow ? rowCount - 1 : rowCount;
    }
}