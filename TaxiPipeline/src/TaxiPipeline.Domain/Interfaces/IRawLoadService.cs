using TaxiPipeline.Domain.Entities;

namespace TaxiPipeline.Domain.Interfaces;

public interface IRawLoadService
{
    Task<int> LoadRawDataAsync(long batchId, IReadOnlyList<TripRecord> records, CancellationToken cancellationToken = default);
}
