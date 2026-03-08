namespace TaxiPipeline.Domain.Interfaces;

public interface IStoredProcedureExecutor
{
    Task<int> ExecuteWithRowCountAsync(string procedureName, long batchId, string outputParameterName, CancellationToken cancellationToken = default);
}
