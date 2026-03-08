using Microsoft.Data.SqlClient;

var masterCs = "Server=tonfly.cloud;User Id=sa;Password=Aa@@91684895;TrustServerCertificate=True;Connection Timeout=30;";
var dbCs = "Server=tonfly.cloud;Database=TaxiPipelineDB;User Id=sa;Password=Aa@@91684895;TrustServerCertificate=True;Connection Timeout=30;";
var basePath = @"C:\Users\tonfly\sql_server\TaxiPipeline\sql";

// Scripts to run on master (database creation)
var masterScripts = new[] { @"01_database\001_create_database.sql" };

// Scripts to run on TaxiPipelineDB
var dbScripts = new[]
{
    @"02_schemas\001_create_schemas.sql",
    @"03_tables\001_ops_tables.sql",
    @"03_tables\002_landing_tables.sql",
    @"03_tables\003_staging_tables.sql",
    @"03_tables\004_core_tables.sql",
    @"04_indexes\001_create_indexes.sql",
    @"05_stored_procedures\ops\001_usp_start_batch.sql",
    @"05_stored_procedures\ops\002_usp_finish_batch.sql",
    @"05_stored_procedures\ops\003_usp_log_error.sql",
    @"05_stored_procedures\landing\001_usp_insert_yellow_trip_raw.sql",
    @"05_stored_procedures\staging\001_usp_clean_yellow_trip_data.sql",
    @"05_stored_procedures\staging\002_usp_reject_invalid_yellow_trip_data.sql",
    @"05_stored_procedures\staging\003_usp_deduplicate_yellow_trip_data.sql",
    @"05_stored_procedures\core\001_usp_load_trip.sql",
};

void RunScripts(string connectionString, string[] scripts, string label)
{
    Console.WriteLine($"\n=== {label} ===");
    using var conn = new SqlConnection(connectionString);
    conn.Open();

    foreach (var script in scripts)
    {
        var fullPath = Path.Combine(basePath, script);
        if (!File.Exists(fullPath))
        {
            Console.WriteLine($"  [SKIP] {script} - file not found");
            continue;
        }

        var sql = File.ReadAllText(fullPath);
        // Split by GO batches
        var batches = System.Text.RegularExpressions.Regex.Split(sql, @"^\s*GO\s*$",
            System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        int batchCount = 0;
        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (string.IsNullOrWhiteSpace(trimmed)) continue;

            try
            {
                using var cmd = new SqlCommand(trimmed, conn) { CommandTimeout = 60 };
                cmd.ExecuteNonQuery();
                batchCount++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [ERROR] {script} batch {batchCount + 1}: {ex.Message}");
            }
        }
        Console.WriteLine($"  [OK] {script} ({batchCount} batches)");
    }
}

try
{
    RunScripts(masterCs, masterScripts, "MASTER - Create Database");
    RunScripts(dbCs, dbScripts, "TaxiPipelineDB - Create Objects");
    Console.WriteLine("\n=== ALL SCRIPTS COMPLETED ===");
}
catch (Exception ex)
{
    Console.WriteLine($"\nFATAL: {ex.Message}");
}
