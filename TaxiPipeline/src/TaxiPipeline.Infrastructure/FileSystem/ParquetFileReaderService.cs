using Parquet;
using Parquet.Data;
using Parquet.Schema;
using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.FileSystem;

public class ParquetFileReaderService : IFileReaderService
{
    private readonly AppSettings _settings;

    private static readonly Dictionary<string, int> ColumnMap = new(StringComparer.OrdinalIgnoreCase)
    {
        { "VendorID",                   0 },
        { "vendor_id",                  0 },
        { "tpep_pickup_datetime",       1 },
        { "pickup_datetime",            1 },
        { "tpep_dropoff_datetime",      2 },
        { "dropoff_datetime",           2 },
        { "passenger_count",            3 },
        { "trip_distance",              4 },
        { "RatecodeID",                 5 },
        { "rate_code",                  5 },
        { "rate_code_id",               5 },
        { "store_and_fwd_flag",         6 },
        { "PULocationID",              7 },
        { "pickup_location_id",         7 },
        { "DOLocationID",              8 },
        { "dropoff_location_id",        8 },
        { "payment_type",               9 },
        { "fare_amount",               10 },
        { "extra",                     11 },
        { "mta_tax",                   12 },
        { "tip_amount",                13 },
        { "tolls_amount",              14 },
        { "improvement_surcharge",     15 },
        { "total_amount",              16 },
        { "congestion_surcharge",      17 },
        { "airport_fee",               18 }
    };

    public ParquetFileReaderService(AppSettings settings)
    {
        _settings = settings;
    }

    public async Task<IReadOnlyList<TripRecord>> ReadFileAsync(
        string filePath,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Parquet file not found: {filePath}");

        var records = new List<TripRecord>();
        int maxRecords = _settings.MaxRecordsFromApi;

        using var stream = File.OpenRead(filePath);
        using var reader = await ParquetReader.CreateAsync(stream, cancellationToken: cancellationToken);

        var dataFields = reader.Schema.GetDataFields();

        var fieldPositions = new Dictionary<int, int>();
        for (int i = 0; i < dataFields.Length; i++)
        {
            if (ColumnMap.TryGetValue(dataFields[i].Name, out int fieldIndex))
            {
                fieldPositions[fieldIndex] = i;
            }
        }

        int lineNumber = 0;

        for (int g = 0; g < reader.RowGroupCount; g++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var groupReader = reader.OpenRowGroupReader(g);

            var columns = new DataColumn[dataFields.Length];
            for (int c = 0; c < dataFields.Length; c++)
            {
                columns[c] = await groupReader.ReadColumnAsync(dataFields[c], cancellationToken);
            }

            int rowCount = columns[0].Data.Length;

            for (int r = 0; r < rowCount; r++)
            {
                lineNumber++;

                if (maxRecords > 0 && lineNumber > maxRecords)
                    return records.AsReadOnly();

                var record = MapRow(columns, fieldPositions, r, lineNumber);
                records.Add(record);
            }
        }

        return records.AsReadOnly();
    }

    private static TripRecord MapRow(
        DataColumn[] columns,
        Dictionary<int, int> fieldPositions,
        int rowIndex,
        int lineNumber)
    {
        string? GetField(int fieldIndex)
        {
            if (!fieldPositions.TryGetValue(fieldIndex, out int colIndex))
                return null;

            var value = columns[colIndex].Data.GetValue(rowIndex);
            if (value == null)
                return null;

            return FormatValue(value);
        }

        return new TripRecord
        {
            SourceLineNumber     = lineNumber,
            VendorId             = GetField(0),
            PickupDatetime       = GetField(1),
            DropoffDatetime      = GetField(2),
            PassengerCount       = GetField(3),
            TripDistance          = GetField(4),
            RateCode             = GetField(5),
            StoreAndFwdFlag      = GetField(6),
            PickupLocationId     = GetField(7),
            DropoffLocationId    = GetField(8),
            PaymentType          = GetField(9),
            FareAmount           = GetField(10),
            Extra                = GetField(11),
            MtaTax               = GetField(12),
            TipAmount            = GetField(13),
            TollsAmount          = GetField(14),
            ImprovementSurcharge = GetField(15),
            TotalAmount          = GetField(16),
            CongestionSurcharge  = GetField(17),
            AirportFee           = GetField(18)
        };
    }

    private static string FormatValue(object value)
    {
        return value switch
        {
            DateTimeOffset dto => dto.DateTime.ToString("yyyy-MM-dd HH:mm:ss"),
            DateTime dt        => dt.ToString("yyyy-MM-dd HH:mm:ss"),
            double d           => d.ToString("G"),
            float f            => f.ToString("G"),
            decimal dec        => dec.ToString("G"),
            _                  => value.ToString() ?? string.Empty
        };
    }
}
