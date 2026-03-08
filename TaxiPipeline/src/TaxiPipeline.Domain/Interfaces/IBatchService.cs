using TaxiPipeline.Domain.Entities;

namespace TaxiPipeline.Domain.Interfaces;

public interface IBatchService
{
    Task<long> StartBatchAsync(string sourceFileName, CancellationToken cancellationToken = default);

    Task FinishBatchAsync(BatchContext context, bool success, string? errorMessage = null, CancellationToken cancellationToken = default);
}
