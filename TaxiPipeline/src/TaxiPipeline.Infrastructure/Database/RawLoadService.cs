using System.Data;
using Microsoft.Data.SqlClient;
using TaxiPipeline.Domain.Entities;
using TaxiPipeline.Domain.Interfaces;

namespace TaxiPipeline.Infrastructure.Database;

public class RawLoadService : IRawLoadService
{
    private readonly SqlConnectionFactory _connectionFactory;
    private readonly AppSettings _settings;

    public RawLoadService(SqlConnectionFactory connectionFactory, AppSettings settings)
    {
        _connectionFactory = connectionFactory;
        _settings = settings;
    }

    public async Task<int> LoadRawDataAsync(
        long batchId,
        IReadOnlyList<TripRecord> records,
        CancellationToken cancellationToken = default)
    {
        if (records.Count == 0) return 0;

        var dataTable = BuildDataTable(batchId, records);

        await using var connection = await _connectionFactory.CreateOpenConnectionAsync(cancellationToken);

        using var bulkCopy = new SqlBulkCopy(connection)
        {
            DestinationTableName = "landing.YellowTripRaw",
            BatchSize = _settings.BulkCopyBatchSize,
            BulkCopyTimeout = _settings.BulkCopyTimeoutSeconds,
            EnableStreaming = true
        };

        MapColumns(bulkCopy);

        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

        return records.Count;
    }

    private static DataTable BuildDataTable(long batchId, IReadOnlyList<TripRecord> records)
    {
        var dt = new DataTable();

        dt.Columns.Add("batch_id", typeof(long));
        dt.Columns.Add("source_line_number", typeof(int));
        dt.Columns.Add("vendor_id", typeof(string));
        dt.Columns.Add("pickup_datetime", typeof(string));
        dt.Columns.Add("dropoff_datetime", typeof(string));
        dt.Columns.Add("passenger_count", typeof(string));
        dt.Columns.Add("trip_distance", typeof(string));
        dt.Columns.Add("rate_code", typeof(string));
        dt.Columns.Add("store_and_fwd_flag", typeof(string));
        dt.Columns.Add("pickup_location_id", typeof(string));
        dt.Columns.Add("dropoff_location_id", typeof(string));
        dt.Columns.Add("payment_type", typeof(string));
        dt.Columns.Add("fare_amount", typeof(string));
        dt.Columns.Add("extra", typeof(string));
        dt.Columns.Add("mta_tax", typeof(string));
        dt.Columns.Add("tip_amount", typeof(string));
        dt.Columns.Add("tolls_amount", typeof(string));
        dt.Columns.Add("improvement_surcharge", typeof(string));
        dt.Columns.Add("total_amount", typeof(string));
        dt.Columns.Add("congestion_surcharge", typeof(string));
        dt.Columns.Add("airport_fee", typeof(string));

        foreach (var r in records)
        {
            dt.Rows.Add(
                batchId,
                r.SourceLineNumber,
                NullIfEmpty(r.VendorId),
                NullIfEmpty(r.PickupDatetime),
                NullIfEmpty(r.DropoffDatetime),
                NullIfEmpty(r.PassengerCount),
                NullIfEmpty(r.TripDistance),
                NullIfEmpty(r.RateCode),
                NullIfEmpty(r.StoreAndFwdFlag),
                NullIfEmpty(r.PickupLocationId),
                NullIfEmpty(r.DropoffLocationId),
                NullIfEmpty(r.PaymentType),
                NullIfEmpty(r.FareAmount),
                NullIfEmpty(r.Extra),
                NullIfEmpty(r.MtaTax),
                NullIfEmpty(r.TipAmount),
                NullIfEmpty(r.TollsAmount),
                NullIfEmpty(r.ImprovementSurcharge),
                NullIfEmpty(r.TotalAmount),
                NullIfEmpty(r.CongestionSurcharge),
                NullIfEmpty(r.AirportFee)
            );
        }

        return dt;
    }

    private static void MapColumns(SqlBulkCopy bulkCopy)
    {
        bulkCopy.ColumnMappings.Add("batch_id", "batch_id");
        bulkCopy.ColumnMappings.Add("source_line_number", "source_line_number");
        bulkCopy.ColumnMappings.Add("vendor_id", "vendor_id");
        bulkCopy.ColumnMappings.Add("pickup_datetime", "pickup_datetime");
        bulkCopy.ColumnMappings.Add("dropoff_datetime", "dropoff_datetime");
        bulkCopy.ColumnMappings.Add("passenger_count", "passenger_count");
        bulkCopy.ColumnMappings.Add("trip_distance", "trip_distance");
        bulkCopy.ColumnMappings.Add("rate_code", "rate_code");
        bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "store_and_fwd_flag");
        bulkCopy.ColumnMappings.Add("pickup_location_id", "pickup_location_id");
        bulkCopy.ColumnMappings.Add("dropoff_location_id", "dropoff_location_id");
        bulkCopy.ColumnMappings.Add("payment_type", "payment_type");
        bulkCopy.ColumnMappings.Add("fare_amount", "fare_amount");
        bulkCopy.ColumnMappings.Add("extra", "extra");
        bulkCopy.ColumnMappings.Add("mta_tax", "mta_tax");
        bulkCopy.ColumnMappings.Add("tip_amount", "tip_amount");
        bulkCopy.ColumnMappings.Add("tolls_amount", "tolls_amount");
        bulkCopy.ColumnMappings.Add("improvement_surcharge", "improvement_surcharge");
        bulkCopy.ColumnMappings.Add("total_amount", "total_amount");
        bulkCopy.ColumnMappings.Add("congestion_surcharge", "congestion_surcharge");
        bulkCopy.ColumnMappings.Add("airport_fee", "airport_fee");
    }

    private static object NullIfEmpty(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return DBNull.Value;
        return value.Trim();
    }
}
