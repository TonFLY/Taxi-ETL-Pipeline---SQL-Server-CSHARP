namespace TaxiPipeline.Domain.Entities;

public class BatchContext
{
    public long BatchId { get; set; }
    public string SourceFileName { get; set; } = string.Empty;
    public int TotalRowsRead { get; set; }
    public int TotalRowsLanded { get; set; }
    public int TotalRowsCleaned { get; set; }
    public int TotalRowsRejected { get; set; }
    public int TotalRowsLoaded { get; set; }

    public string GetSummary()
    {
        return $"Batch {BatchId} | File: {SourceFileName} | " +
               $"Read: {TotalRowsRead} | Landed: {TotalRowsLanded} | " +
               $"Cleaned: {TotalRowsCleaned} | Rejected: {TotalRowsRejected} | " +
               $"Loaded: {TotalRowsLoaded}";
    }
}
