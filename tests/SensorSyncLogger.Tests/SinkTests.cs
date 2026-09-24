using Microsoft.Data.Sqlite;
using SensorSyncLogger.Sinks;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Tests;

public sealed class SinkTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "SensorSyncLoggerTests", Guid.NewGuid().ToString("N"));

    public SinkTests() => Directory.CreateDirectory(_directory);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
            // Best effort cleanup of temporary files.
        }
    }

    private string PathOf(string name) => Path.Combine(_directory, name);

    [Fact]
    public async Task Csv_raw_rows_use_invariant_culture_and_iso_timestamps()
    {
        var sink = new CsvLogSink(PathOf("raw.csv"));
        var schema = TestSchema.Raw("Load,Cell", "Temp");
        await sink.OpenAsync(schema);
        await sink.WriteBatchAsync(new[]
        {
            LogRow.Raw(schema.SessionStartUtc.AddMilliseconds(10), 0, 1234.5),
            LogRow.Raw(schema.SessionStartUtc.AddMilliseconds(20), 1, double.NaN),
        });
        await sink.DisposeAsync();

        var lines = File.ReadAllLines(PathOf("raw.csv"));
        Assert.Equal("timestamp_utc,elapsed_s,channel,value", lines[0]);
        Assert.Equal("2026-01-01T10:00:00.0100000Z,0.01,\"Load,Cell\",1234.5", lines[1]);
        Assert.Equal("2026-01-01T10:00:00.0200000Z,0.02,Temp,", lines[2]);
    }

    [Fact]
    public async Task Csv_aligned_rows_have_a_column_per_channel_and_append_skips_the_header()
    {
        var schema = TestSchema.Aligned("A", "B");
        for (int run = 0; run < 2; run++)
        {
            var sink = new CsvLogSink(PathOf("aligned.csv"), new CsvLogSinkOptions { Append = true, Delimiter = ';' });
            await sink.OpenAsync(schema);
            await sink.WriteBatchAsync(new[] { LogRow.Aligned(schema.SessionStartUtc, new[] { 1.25, double.NaN }) });
            await sink.DisposeAsync();
        }

        var lines = File.ReadAllLines(PathOf("aligned.csv"));
        Assert.Equal(new[]
        {
            "timestamp_utc;elapsed_s;A [u];B [u]",
            "2026-01-01T10:00:00.0000000Z;0.0;1.25;",
            "2026-01-01T10:00:00.0000000Z;0.0;1.25;",
        }, lines);
    }

    [Fact]
    public async Task Sqlite_raw_rows_are_stored_with_channel_metadata()
    {
        string db = PathOf("raw.db");
        var schema = TestSchema.Raw("A", "B");
        var sink = new SqliteLogSink(db);
        await sink.OpenAsync(schema);
        await sink.WriteBatchAsync(Enumerable.Range(0, 500)
            .Select(i => LogRow.Raw(schema.SessionStartUtc.AddMilliseconds(i), i % 2, i == 3 ? double.NaN : i))
            .ToArray());
        await sink.FlushAsync();
        await sink.DisposeAsync();

        using var connection = Open(db);
        Assert.Equal(500L, Scalar(connection, "SELECT COUNT(*) FROM samples"));
        Assert.Equal(1L, Scalar(connection, "SELECT COUNT(*) FROM samples WHERE value IS NULL"));
        Assert.Equal(250L, Scalar(connection, "SELECT COUNT(*) FROM samples_view WHERE channel = 'B'"));
        Assert.Equal("2026-01-01T10:00:00.001Z", Scalar(connection, "SELECT timestamp_utc FROM samples_view WHERE value = 1"));
        Assert.Equal(1_767_261_600_000_000L, Scalar(connection, "SELECT MIN(ts_unix_us) FROM samples"));
    }

    [Fact]
    public async Task Sqlite_aligned_table_gains_columns_for_new_channels_in_later_sessions()
    {
        string db = PathOf("aligned.db");

        var first = new SqliteLogSink(db);
        var schemaA = TestSchema.Aligned("A");
        await first.OpenAsync(schemaA);
        await first.WriteBatchAsync(new[] { LogRow.Aligned(schemaA.SessionStartUtc, new[] { 1.0 }) });
        await first.DisposeAsync();

        var second = new SqliteLogSink(db);
        var schemaAB = TestSchema.Aligned("A", "B");
        await second.OpenAsync(schemaAB);
        await second.WriteBatchAsync(new[] { LogRow.Aligned(schemaAB.SessionStartUtc, new[] { 2.0, double.NaN }) });
        await second.DisposeAsync();

        Assert.NotEqual(first.SessionId, second.SessionId);
        using var connection = Open(db);
        Assert.Equal(2L, Scalar(connection, "SELECT COUNT(*) FROM aligned_samples"));
        Assert.Equal(2.0, Scalar(connection, $"SELECT A FROM aligned_samples WHERE session_id = {second.SessionId}"));
        Assert.Equal(DBNull.Value, Scalar(connection, $"SELECT B FROM aligned_samples WHERE session_id = {second.SessionId}"));
        Assert.Equal(2L, Scalar(connection, "SELECT COUNT(*) FROM sessions"));
    }

    private static SqliteConnection Open(string path)
    {
        var connection = new SqliteConnection($"Data Source={path};Pooling=False");
        connection.Open();
        return connection;
    }

    private static object Scalar(SqliteConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }
}
