using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.FileSystem;

public class CsvFileReaderService : IFileReaderService
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

    public CsvFileReaderService(AppSettings settings)
    {
        _settings = settings;
    }

    public async Task<IReadOnlyList<TripRecord>> ReadFileAsync(
        string filePath,
        CancellationToken cancellationToken = default)
    {
        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Input file not found: {filePath}");

        var records = new List<TripRecord>();
        var delimiter = _settings.CsvDelimiter.ToCharArray();

        using var reader = new StreamReader(filePath, System.Text.Encoding.UTF8);

        var headerLine = await reader.ReadLineAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(headerLine))
            throw new InvalidDataException("CSV file has no header row.");

        var headers = headerLine.Split(delimiter);
        var columnIndexMap = BuildColumnIndexMap(headers);

        int lineNumber = 1;
        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync(cancellationToken);
            lineNumber++;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            var fields = line.Split(delimiter);
            var record = MapToTripRecord(fields, columnIndexMap, lineNumber);
            records.Add(record);
        }

        return records.AsReadOnly();
    }

    private static Dictionary<int, int> BuildColumnIndexMap(string[] headers)
    {
        var map = new Dictionary<int, int>();

        for (int i = 0; i < headers.Length; i++)
        {
            var headerName = headers[i].Trim().Trim('"');
            if (ColumnMap.TryGetValue(headerName, out int fieldIndex))
            {
                map[fieldIndex] = i;
            }
        }

        return map;
    }

    private static TripRecord MapToTripRecord(string[] fields, Dictionary<int, int> columnIndexMap, int lineNumber)
    {
        string? GetField(int fieldIndex)
        {
            if (columnIndexMap.TryGetValue(fieldIndex, out int csvIndex) && csvIndex < fields.Length)
            {
                var val = fields[csvIndex].Trim().Trim('"');
                return string.IsNullOrWhiteSpace(val) ? null : val;
            }
            return null;
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
}
