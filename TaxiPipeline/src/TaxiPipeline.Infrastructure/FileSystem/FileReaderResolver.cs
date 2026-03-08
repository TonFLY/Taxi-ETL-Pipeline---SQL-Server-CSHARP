using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.FileSystem;

public class FileReaderResolver : IFileReaderService
{
    private readonly CsvFileReaderService _csvReader;
    private readonly ParquetFileReaderService _parquetReader;

    public FileReaderResolver(AppSettings settings)
    {
        _csvReader = new CsvFileReaderService(settings);
        _parquetReader = new ParquetFileReaderService(settings);
    }

    public Task<IReadOnlyList<TripRecord>> ReadFileAsync(
        string filePath,
        CancellationToken cancellationToken = default)
    {
        var extension = Path.GetExtension(filePath);

        if (extension.Equals(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            return _parquetReader.ReadFileAsync(filePath, cancellationToken);
        }

        return _csvReader.ReadFileAsync(filePath, cancellationToken);
    }
}
