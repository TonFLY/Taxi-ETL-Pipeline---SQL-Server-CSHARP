namespace TaxiPipeline.Domain.Entities;

public class AppSettings
{
    public string ConnectionString { get; set; } = string.Empty;
    public string InputDirectory { get; set; } = string.Empty;
    public string ArchiveDirectory { get; set; } = string.Empty;
    public string FilePattern { get; set; } = "*.csv";
    public int BulkCopyBatchSize { get; set; } = 5000;
    public int BulkCopyTimeoutSeconds { get; set; } = 120;
    public bool ArchiveAfterProcessing { get; set; } = true;
    public string CsvDelimiter { get; set; } = ",";

    public string ApiBaseUrl { get; set; } = "https://d37ci6vzurychx.cloudfront.net/trip-data/";

    public string ApiFileNamePattern { get; set; } = "yellow_tripdata_{0}-{1:D2}.parquet";

    public int MaxRecordsFromApi { get; set; } = 0;

    public string DownloadDirectory { get; set; } = string.Empty;
}
