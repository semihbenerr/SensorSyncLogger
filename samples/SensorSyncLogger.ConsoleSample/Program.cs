using System.Diagnostics;
using System.Globalization;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using SensorSyncLogger.Sinks;
using SensorSyncLogger.Sources;
using SensorSyncLogger.Sync;
using SensorSyncLogger.Timing;
using SensorSyncLogger.Writing;

// Simulates three sensors at different rates and logs them for 10 seconds to CSV and SQLite at the same time:
//   - raw rows    (every sample with its own timestamp)  -> output/raw.csv     + output/sensors.db (samples)
//   - aligned rows (one row per 100 ms, last known value) -> output/aligned.csv + output/sensors.db (aligned_samples)
//
// Usage: SensorSyncLogger.ConsoleSample [--duration <seconds>] [--output <folder>]     Ctrl+C stops early.

var duration = TimeSpan.FromSeconds(10);
string outputDirectory = Path.Combine(Environment.CurrentDirectory, "output");
for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--duration" when i + 1 < args.Length
            && double.TryParse(args[i + 1], NumberStyles.Float, CultureInfo.InvariantCulture, out double seconds) && seconds > 0:
            duration = TimeSpan.FromSeconds(seconds);
            i++;
            break;
        case "--output" when i + 1 < args.Length:
            outputDirectory = Path.GetFullPath(args[++i]);
            break;
        default:
            Console.Error.WriteLine("Usage: SensorSyncLogger.ConsoleSample [--duration <seconds>] [--output <folder>]");
            return 1;
    }
}

Console.OutputEncoding = System.Text.Encoding.UTF8; // units such as °C
Directory.CreateDirectory(outputDirectory);
string rawCsv = Path.Combine(outputDirectory, "raw.csv");
string alignedCsv = Path.Combine(outputDirectory, "aligned.csv");
string database = Path.Combine(outputDirectory, "sensors.db");

using var loggerFactory = LoggerFactory.Create(b => b
    .AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; })
    .SetMinimumLevel(LogLevel.Information));

// --- Simulated sensors -------------------------------------------------------------------------------------------
var clock = SyncClock.Default;
var start = clock.UtcNow;
var random = new Random(42);
double Seconds(DateTime t) => (t - start).TotalSeconds;
double Noise(double amplitude)
{
    lock (random)
        return (random.NextDouble() * 2 - 1) * amplitude;
}

var loadCell = new FuncSensorSource("LoadCell", 100, t => 1000 + 500 * Math.Sin(2 * Math.PI * 0.5 * Seconds(t)) + Noise(2), "N");
var temperature = new FuncSensorSource("Temperature", 10, t => 25 + 0.05 * Seconds(t) + Noise(0.05), "°C");
var pressure = new FuncSensorSource("Pressure", 1, t => 6 + 0.2 * Math.Sin(2 * Math.PI * Seconds(t) / 10) + Noise(0.01), "bar");
ISensorSource[] sources = { loadCell, temperature, pressure };

// --- Two engines sharing the same sources ------------------------------------------------------------------------
await using var rawEngine = new TimeSyncEngine(
    new TimeSyncOptions { Mode = LogMode.Raw, ManageSourceLifetime = true },
    loggerFactory.CreateLogger("Raw"));

await using var alignedEngine = new TimeSyncEngine(
    new TimeSyncOptions
    {
        Mode = LogMode.Aligned,
        AlignInterval = TimeSpan.FromMilliseconds(100),
        Interpolation = InterpolationMode.LastKnownValue,
        ManageSourceLifetime = false, // the raw engine starts and stops the sources
    },
    loggerFactory.CreateLogger("Aligned"));

foreach (var source in sources)
{
    rawEngine.AddSource(source);
    alignedEngine.AddSource(source);
}

rawEngine.AddSink(new CsvLogSink(rawCsv)).AddSink(new SqliteLogSink(database));
alignedEngine.AddSink(new CsvLogSink(alignedCsv)).AddSink(new SqliteLogSink(database));

using var cts = new CancellationTokenSource(duration);
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Console.WriteLine($"Logging {sources.Length} channels for {duration.TotalSeconds:0.#} s to {outputDirectory}");
Console.WriteLine($"High-resolution timer: {(OperatingSystem.IsWindows() ? "Windows waitable timer" : "OS sleep")}; Ctrl+C stops early.");
Console.WriteLine();

await alignedEngine.StartAsync(); // subscribe first so it sees the very first samples
await rawEngine.StartAsync();     // starts the sources

var stopwatch = Stopwatch.StartNew();
try
{
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    while (await timer.WaitForNextTickAsync(cts.Token))
        PrintStatus(stopwatch.Elapsed, rawEngine.GetStatus(), alignedEngine.GetStatus());
}
catch (OperationCanceledException)
{
    // Duration elapsed or Ctrl+C.
}

// --- Graceful shutdown: stop sources, write everything, close files ----------------------------------------------
await rawEngine.StopAsync();
await alignedEngine.StopAsync();

var rawStatus = rawEngine.GetStatus();
var alignedStatus = alignedEngine.GetStatus();
Console.WriteLine();
Console.WriteLine("=== Final status ===");
Console.WriteLine("Raw:     " + rawStatus);
Console.WriteLine("Aligned: " + alignedStatus);
Console.WriteLine();

// --- Verify what reached the disk --------------------------------------------------------------------------------
long rawCsvRows = CountCsvRows(rawCsv);
long alignedCsvRows = CountCsvRows(alignedCsv);
var (dbRaw, dbAligned) = CountDatabaseRows(database);

Console.WriteLine("=== Output ===");
Console.WriteLine($"{rawCsv}: {rawCsvRows} rows ({new FileInfo(rawCsv).Length / 1024.0:0.#} KB)");
Console.WriteLine($"{alignedCsv}: {alignedCsvRows} rows ({new FileInfo(alignedCsv).Length / 1024.0:0.#} KB)");
Console.WriteLine($"{database}: {dbRaw} raw rows, {dbAligned} aligned rows in this session");
Console.WriteLine();
Console.WriteLine("First aligned rows:");
foreach (var line in File.ReadLines(alignedCsv).Take(6))
    Console.WriteLine("  " + line);

bool consistent = rawStatus.RowsProduced == rawStatus.SamplesReceived
    && rawCsvRows == rawStatus.RowsProduced && dbRaw == rawStatus.RowsProduced
    && alignedCsvRows == alignedStatus.RowsProduced && dbAligned == alignedStatus.RowsProduced
    && rawStatus.Buffer!.RowsDropped == 0 && alignedStatus.Buffer!.RowsDropped == 0;
Console.WriteLine();
Console.WriteLine(consistent
    ? "OK: every sample and every aligned row reached both the CSV files and the database."
    : "MISMATCH: some rows are missing, see the counters above.");
return consistent ? 0 : 2;

static void PrintStatus(TimeSpan elapsed, SyncStatus raw, SyncStatus aligned)
{
    string channels = string.Join("  ", raw.Channels.Select(c =>
        string.Create(CultureInfo.InvariantCulture, $"{c.Name} {c.MeasuredRateHz,5:0.0} Hz = {c.LastValue,8:0.00} {c.Unit}")));
    Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
        $"[{elapsed.TotalSeconds,4:0}s] {channels} | raw written {raw.Buffer?.RowsWritten,5} (buffer {raw.Buffer?.FillRatio,6:P2}) | aligned rows {aligned.RowsProduced,4}"));
}

static long CountCsvRows(string path) => File.ReadLines(path).LongCount() - 1; // minus header

static (long Raw, long Aligned) CountDatabaseRows(string path)
{
    using var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = path, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ToString());
    connection.Open();

    long Scalar(string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    // Each engine created its own session; the newest Raw and Aligned sessions belong to this run.
    long rawSession = Scalar("SELECT MAX(session_id) FROM sessions WHERE mode = 'Raw'");
    long alignedSession = Scalar("SELECT MAX(session_id) FROM sessions WHERE mode = 'Aligned'");
    return (Scalar($"SELECT COUNT(*) FROM samples WHERE session_id = {rawSession}"),
            Scalar($"SELECT COUNT(*) FROM aligned_samples WHERE session_id = {alignedSession}"));
}
