# SensorSyncLogger

Synchronise sensors that run at **different sample rates** — a 100 Hz load cell, a 10 Hz thermocouple, a 1 Hz
pressure transmitter — on one high-resolution time base, and log them to **CSV and SQLite** without ever blocking
the acquisition threads. .NET 8, MIT licensed.

```bash
dotnet add package SensorSyncLogger
```

- **One time base:** `SyncClock` is Stopwatch-based and monotonic (never jumps with NTP/clock changes), with sub-µs resolution
- **Two modes:** *Raw* (every sample with its own timestamp) or *Aligned* (one row per interval, a column per channel,
  last-known-value or linear interpolation on a clean time grid)
- **Non-blocking:** sensor callbacks only enqueue into a lock-free queue; a background writer flushes in batches
  by row count or time
- **CSV sink:** streaming, constant memory, invariant culture, ISO 8601 timestamps with 100 ns precision
- **SQLite sink:** batched inserts in one transaction with prepared commands, WAL mode, several sessions per file
- **Graceful shutdown:** stop/dispose writes every accepted row before closing the files
- **Live status:** samples, measured rates, rows written, buffer fill ratio, per-sink errors
- **Precise periodic sources:** `FuncSensorSource` / `PeriodicSensorSource` pace loops with a Windows
  high-resolution timer (100 Hz is not possible with the default 15.6 ms timer)

## Quick start

```csharp
using SensorSyncLogger.Sinks;
using SensorSyncLogger.Sources;
using SensorSyncLogger.Sync;
using SensorSyncLogger.Writing;

var loadCell = new FuncSensorSource("LoadCell", 100, t => ReadLoadCell(), "N");
var temperature = new FuncSensorSource("Temperature", 10, t => ReadTemperature(), "°C");

await using var engine = new TimeSyncEngine(new TimeSyncOptions
{
    Mode = LogMode.Aligned,                          // or LogMode.Raw
    AlignInterval = TimeSpan.FromMilliseconds(100),
    Interpolation = InterpolationMode.LastKnownValue // or Linear
});

engine.AddSource(loadCell)
      .AddSource(temperature)
      .AddSink(new CsvLogSink("run.csv"))
      .AddSink(new SqliteLogSink("run.db"));

await engine.StartAsync();
// ... test runs ...
Console.WriteLine(engine.GetStatus());   // samples, rates, rows written, buffer fill
await engine.StopAsync();                // writes everything that is still buffered
```

## Your own sensors

Implement `ISensorSource`, or derive from `SensorSourceBase` and call `Publish(value)` whenever a value arrives
(e.g. from a serial port event). Samples are stamped with `SyncClock.Default`, so all channels share one time base.

```csharp
public sealed class SerialLoadCell : SensorSourceBase
{
    public SerialLoadCell() : base("LoadCell", sampleRateHz: 100, unit: "N") { }

    public void OnFrameReceived(double newtons) => Publish(newtons);
}
```

For polled devices, derive from `PeriodicSensorSource` and implement `ReadValue`; it runs on its own thread at the
requested rate, skips (and counts) ticks when it falls behind, and keeps running if a read throws.

## Output

**CSV, raw mode:** `timestamp_utc,elapsed_s,channel,value`
**CSV, aligned mode:** `timestamp_utc,elapsed_s,LoadCell [N],Temperature [°C],…` (empty field = no value)

**SQLite:** `sessions`, `channels`, `samples` (+ readable `samples_view`) and `aligned_samples` (a column per channel).
Timestamps are integer microseconds since the Unix epoch.

```sql
SELECT * FROM samples_view WHERE session_id = 1 AND channel = 'LoadCell' LIMIT 10;
```

## Tuning

| Option | Default | Meaning |
|---|---|---|
| `Writer.BatchSize` | 1000 | Rows per write; a flush starts when this many are waiting |
| `Writer.FlushInterval` | 1 s | Maximum time rows stay in memory |
| `Writer.Capacity` | 500 000 | Maximum buffered rows; `OverflowPolicy` decides what is dropped beyond it |
| `AlignmentDelay` | 1 interval | Time a grid point waits for late samples (longer for `Linear`) |
| `MaxSampleAge` | none | Values older than this become empty instead of being held forever |
| `ManageSourceLifetime` | true | Engine starts/stops sources; set `false` to share sources between engines |

## Sample

`samples/SensorSyncLogger.ConsoleSample` simulates a 100 Hz load cell, a 10 Hz temperature and a 1 Hz pressure
sensor for 10 seconds and writes raw and aligned data to CSV and SQLite at the same time, then verifies the files:

```bash
dotnet run --project samples/SensorSyncLogger.ConsoleSample
```

## License

MIT © Semih Bener
