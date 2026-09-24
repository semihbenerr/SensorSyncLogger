# SensorSyncLogger

**🇬🇧 Synchronise sensors running at different sample rates on one high-resolution time base and log them to CSV and SQLite — without ever blocking your acquisition threads.**

**🇹🇷 Farklı örnekleme frekanslarında çalışan sensörleri tek bir yüksek çözünürlüklü zaman ekseninde senkronize edin ve veri toplama thread'lerini hiç bloklamadan CSV ve SQLite'a loglayın.**

```bash
dotnet add package SensorSyncLogger
```

.NET 8 · MIT · English documentation first, Türkçe dokümantasyon aşağıda.

---

# 🇬🇧 English

## Contents

1. What problem does it solve?
2. Features
3. Quick start
4. Core concepts
5. Raw mode vs. aligned mode
6. Connecting your own sensors
7. Output formats (CSV and SQLite)
8. Configuration reference
9. Monitoring with GetStatus()
10. Shutdown, errors and data safety
11. Using BufferedWriter and sinks on their own
12. Performance
13. Platform notes
14. FAQ

## What problem does it solve?

A typical test bench has sensors that run at very different speeds: a load cell at 100 Hz, a thermocouple at 10 Hz,
a pressure transmitter at 1 Hz. Logging them correctly is harder than it looks:

- **Time stamps must be comparable.** `DateTime.Now` can jump when the system clock is corrected and its effective
  resolution may be coarse, so channels drift apart or even go "back in time".
- **Rates differ.** To compare channels you often need one row per time step with all channels — but the 1 Hz channel
  only has a value every second.
- **Disk I/O must not disturb acquisition.** If a sensor callback writes to a file or database, a slow disk directly
  delays the next sample.
- **Nothing may be lost at the end.** When the test stops, data still in memory must reach the disk.

SensorSyncLogger solves these four problems in one small library.

## Features

- **`SyncClock`** – one Stopwatch-based, monotonic UTC time base with sub-microsecond resolution for all channels
- **Two log modes** – *Raw* (every sample with its own time stamp) and *Aligned* (one row per interval with a column
  per channel, using last-known-value or linear interpolation on a clean time grid)
- **Non-blocking** – sensor callbacks only put the sample into a lock-free queue; a single background writer does all I/O
- **Batched writing** – flush when a number of rows is waiting or after a time interval, whichever comes first
- **CSV sink** – streaming writer with constant memory use, invariant culture, ISO 8601 time stamps (100 ns precision)
- **SQLite sink** – every batch in one transaction with a prepared command, WAL journal, several sessions per database
- **Graceful shutdown** – stopping writes every row that was accepted, then closes the files
- **Live status** – samples received, measured rate per channel, rows written, buffer fill ratio, errors per sink
- **Precise periodic sources** – `FuncSensorSource` / `PeriodicSensorSource` sample at 100 Hz and more on Windows,
  where ordinary timers only tick every 15.6 ms
- **Isolation** – a failing sink (full disk, locked database) does not stop the other sinks

## Quick start

```csharp
using SensorSyncLogger.Sinks;
using SensorSyncLogger.Sources;
using SensorSyncLogger.Sync;
using SensorSyncLogger.Writing;

// 1. Sensors: here polled by delegates; see "Connecting your own sensors" for event-driven devices.
var loadCell    = new FuncSensorSource("LoadCell",    100, t => ReadLoadCell(),    "N");
var temperature = new FuncSensorSource("Temperature",  10, t => ReadTemperature(), "°C");
var pressure    = new FuncSensorSource("Pressure",      1, t => ReadPressure(),    "bar");

// 2. Engine: aligned rows every 100 ms.
await using var engine = new TimeSyncEngine(new TimeSyncOptions
{
    Mode = LogMode.Aligned,
    AlignInterval = TimeSpan.FromMilliseconds(100),
    Interpolation = InterpolationMode.LastKnownValue,
});

engine.AddSource(loadCell)
      .AddSource(temperature)
      .AddSource(pressure)
      .AddSink(new CsvLogSink("test-run.csv"))
      .AddSink(new SqliteLogSink("test-run.db"));

// 3. Run.
await engine.StartAsync();          // opens files, starts the sensors
await Task.Delay(TimeSpan.FromSeconds(10));
Console.WriteLine(engine.GetStatus());
await engine.StopAsync();           // stops the sensors, writes everything, closes the files
```

## Core concepts

| Type | Role |
|---|---|
| `SyncClock` | Shared time base. `SyncClock.Default.UtcNow` gives a monotonic UTC time with Stopwatch resolution. |
| `ISensorSource` | A channel that raises `OnSample` for every new value. Has a `ChannelName`, `SampleRateHz` and optional `Unit`. |
| `SensorSample` | `(DateTime Timestamp, string ChannelName, double Value)` – `double.NaN` marks an invalid value. |
| `TimeSyncEngine` | Subscribes to the sources, produces raw or aligned rows and hands them to the writer. |
| `BufferedWriter` | In-memory queue plus background task that writes batches to the sinks. |
| `ILogSink` | A destination: `CsvLogSink`, `SqliteLogSink`, or your own. |

Data flow:

```
sensor thread ──OnSample──► TimeSyncEngine ──TryEnqueue (lock-free, no I/O)──► BufferedWriter queue
                                                                                   │ background task
                                                                                   ▼
                                                                    CsvLogSink    SqliteLogSink   (in parallel)
```

**Why a special clock?** `SyncClock` reads the wall clock once when it is created and from then on advances only by
`Stopwatch` ticks. Time stamps therefore never jump backwards and keep sub-microsecond resolution. Over many hours the
Stopwatch may drift a few milliseconds from the wall clock; within one test run consistent relative timing is what
matters, so the clock is deliberately never re-synchronised. Use the same clock (normally `SyncClock.Default`) for all
sources — `SensorSourceBase` and `FuncSensorSource` do this automatically.

## Raw mode vs. aligned mode

### Raw mode (`LogMode.Raw`, default)

Every sample becomes one row with its own time stamp. Nothing is interpolated or lost; each channel keeps its own rate.

```
timestamp_utc,elapsed_s,channel,value
2026-09-24T15:37:23.6123450Z,0.012,LoadCell,1003.42
2026-09-24T15:37:23.6223561Z,0.022,LoadCell,1006.97
2026-09-24T15:37:23.6251002Z,0.025,Temperature,25.03
```

Use it when you need the complete original data (signal analysis, audits, later re-processing).

### Aligned mode (`LogMode.Aligned`)

One row per `AlignInterval` (e.g. every 100 ms) with one column per channel. Rows are placed on "round" times
(…23.700, …23.800) so files from different runs line up.

```
timestamp_utc,elapsed_s,LoadCell [N],Temperature [°C],Pressure [bar]
2026-09-24T15:37:23.7000000Z,0.0845,1146.21,24.966,6.002
2026-09-24T15:37:23.8000000Z,0.1845,1286.87,24.991,6.002
```

How each cell is computed (`Interpolation`):

- **`LastKnownValue`** (default, sample-and-hold) – the newest sample at or before the grid time. Never invents values;
  best for slow channels and step-like signals. In the example the 1 Hz pressure repeats until its next sample.
- **`Linear`** – interpolates between the sample before and the sample after the grid time. Smoother for fast analogue
  signals. If the next sample has not arrived in time, the last known value is used.

Timing details:

- **`AlignmentDelay`** – a grid row is produced only after this delay, so late samples can still be included.
  Default: one interval for `LastKnownValue`; interval + slowest sample period (max 10 s) for `Linear`.
- **`MaxSampleAge`** – if a channel's newest value is older than this at a grid time, the cell is left empty instead
  of repeating a stale value forever (e.g. a sensor stopped answering). Default: unlimited.
- Rows at the very beginning, before any channel delivered data, are not written. Later gaps are kept (empty cells).
- At `StopAsync` the remaining rows up to the newest sample are produced, so the file ends where the data ends.

You can log both modes at the same time by using two engines on the same sources (see the FAQ).

## Connecting your own sensors

### Event-driven devices (serial port, DAQ callback, network stream)

Derive from `SensorSourceBase` and call `Publish` whenever a value arrives. It stamps the value with the shared clock.

```csharp
public sealed class SerialLoadCell : SensorSourceBase
{
    private readonly SerialPort _port;

    public SerialLoadCell(string portName) : base("LoadCell", sampleRateHz: 100, unit: "N")
    {
        _port = new SerialPort(portName, 115200);
        _port.DataReceived += (_, _) =>
        {
            if (double.TryParse(_port.ReadLine(), NumberStyles.Float, CultureInfo.InvariantCulture, out var newtons))
                Publish(newtons);          // or Publish(deviceTimestampUtc, newtons)
        };
    }

    public override Task StartAsync(CancellationToken ct = default) { _port.Open(); return Task.CompletedTask; }
    public override Task StopAsync(CancellationToken ct = default)  { _port.Close(); return Task.CompletedTask; }
}
```

### Polled devices (Modbus, SCPI, a function call)

Derive from `PeriodicSensorSource` and implement `ReadValue`, or pass a delegate to `FuncSensorSource`. Each source runs
on its own thread at the requested rate.

```csharp
public sealed class ModbusPressure : PeriodicSensorSource
{
    private readonly IMyDevice _device;

    public ModbusPressure(IMyDevice device) : base("Pressure", sampleRateHz: 10, unit: "bar") => _device = device;

    protected override double ReadValue(DateTime timestampUtc) => _device.ReadPressure();
}
```

Behaviour of periodic sources:

- Ticks are scheduled on an absolute grid (start + n × period), so the rate does not drift.
- If the loop falls more than `MaxLagPeriods` (default 5) periods behind, missed ticks are skipped instead of being
  produced in a burst; they are counted in `MissedTicks`.
- If `ReadValue` throws, the sample is skipped, `ReadErrors` is incremented, `ReadFailed` is raised and sampling continues.
- `ReadValue` should return within one period.

### Implementing `ISensorSource` directly

Provide `ChannelName`, `SampleRateHz`, optional `Unit`, the `OnSample` event and `StartAsync`/`StopAsync`. Take time
stamps from `SyncClock.Default.UtcNow` so all channels share one time base. `OnSample` may be raised from any thread and
should return quickly (the engine never blocks it).

## Output formats (CSV and SQLite)

### CSV (`CsvLogSink`)

```csharp
new CsvLogSink("run.csv", new CsvLogSinkOptions
{
    Delimiter = ',',               // use ';' for Excel in locales with a decimal comma
    Append = false,                // true: append to an existing file (header only for new files)
    IncludeElapsedSeconds = true,  // adds elapsed_s, handy for plotting
    IncludeUnitsInHeader = true,   // "LoadCell [N]" in aligned mode
    FlushToDisk = false,           // true: force data to the physical disk on every flush (slower)
});
```

- UTF-8 without BOM, `.` as decimal separator, fields quoted per RFC 4180 when needed.
- Time stamps: ISO 8601 UTC with 7 fractional digits, e.g. `2026-09-24T15:37:23.7000000Z`.
- `NaN` (no value) is written as an empty field.
- Memory use does not grow with the file: each batch is formatted into a pooled buffer and written in one call.

### SQLite (`SqliteLogSink`)

```csharp
new SqliteLogSink("run.db", new SqliteLogSinkOptions
{
    RawTableName = "samples",
    AlignedTableName = "aligned_samples",
    CreateIndexes = true,                     // index on (session_id, ts_unix_us)
    SynchronousFull = false,                  // true: survive power loss, slower
    BusyTimeout = TimeSpan.FromSeconds(5),    // wait if another connection writes
});
```

Tables (created automatically; several runs can share one file — each run is a *session*):

| Table | Columns |
|---|---|
| `sessions` | `session_id`, `started_utc`, `mode`, `align_interval_ms`, `machine_name` |
| `channels` | `session_id`, `channel_index`, `name`, `unit`, `sample_rate_hz` |
| `samples` (raw) | `session_id`, `ts_unix_us`, `channel_index`, `value` |
| `samples_view` (raw, readable) | `session_id`, `timestamp_utc`, `channel`, `unit`, `value` |
| `aligned_samples` (aligned) | `session_id`, `ts_unix_us`, one `REAL` column per channel name |

- `ts_unix_us` = microseconds since 1970-01-01 UTC (integer). `NaN` is stored as `NULL`.
- If a later session has new channels, columns are added to `aligned_samples` automatically.
- WAL journaling: you can open and query the database while logging is running.

Useful queries:

```sql
-- all runs
SELECT * FROM sessions ORDER BY session_id DESC;

-- raw data of one channel, human-readable time
SELECT timestamp_utc, value FROM samples_view
WHERE session_id = 1 AND channel = 'LoadCell' ORDER BY timestamp_utc;

-- aligned data with a readable time column
SELECT strftime('%Y-%m-%dT%H:%M:%f', ts_unix_us / 1e6, 'unixepoch') AS time, LoadCell, Temperature, Pressure
FROM aligned_samples WHERE session_id = 2 ORDER BY ts_unix_us;
```

## Configuration reference

### `TimeSyncOptions`

| Option | Default | Meaning |
|---|---|---|
| `Mode` | `Raw` | `Raw` or `Aligned` |
| `AlignInterval` | 100 ms | Grid interval in aligned mode (≥ 1 ms) |
| `Interpolation` | `LastKnownValue` | `LastKnownValue` or `Linear` |
| `AlignmentDelay` | auto | Wait for late samples before a grid row is produced |
| `MaxSampleAge` | unlimited | Older values become empty cells |
| `ManageSourceLifetime` | `true` | The engine starts/stops the sources; `false` to share sources between engines |
| `Writer` | see below | Buffering options |
| `Clock` | `SyncClock.Default` | Must be the clock the sources stamp with |

### `BufferedWriterOptions` (`TimeSyncOptions.Writer`)

| Option | Default | Meaning |
|---|---|---|
| `BatchSize` | 1000 | Rows per write; a write starts as soon as this many rows are waiting |
| `FlushInterval` | 1 s | Maximum time a row stays in memory |
| `Capacity` | 500 000 | Maximum buffered rows (≈ 40 bytes each) |
| `OverflowPolicy` | `DropNewest` | When full: reject new rows (`DropNewest`) or discard the oldest (`DropOldest`) |
| `SinkRetryCount` | 2 | Retries of a failed batch per sink |
| `SinkRetryDelay` | 100 ms | Pause between retries |

## Monitoring with GetStatus()

```csharp
SyncStatus status = engine.GetStatus();

Console.WriteLine($"{status.State}, uptime {status.Uptime}");
Console.WriteLine($"samples {status.SamplesReceived}, rows {status.RowsProduced}");
Console.WriteLine($"written {status.Buffer!.RowsWritten}, dropped {status.Buffer.RowsDropped}, buffer {status.Buffer.FillRatio:P1}");

foreach (var channel in status.Channels)
    Console.WriteLine($"{channel.Name}: {channel.MeasuredRateHz:0.0} Hz (nominal {channel.NominalRateHz}), last {channel.LastValue} {channel.Unit}");

foreach (var sink in status.Buffer.Sinks)
    Console.WriteLine($"{sink.Name}: {sink.RowsWritten} rows, {sink.FailedBatches} failed batches");
```

`status.ToString()` prints all of this in a compact form. `GetStatus()` is cheap and thread-safe; calling it every
second from a UI timer is fine.

## Shutdown, errors and data safety

- **Stop / dispose is graceful.** `StopAsync` (or `await using`) stops the sources, produces the last aligned rows,
  writes every row that was accepted — including rows being enqueued at that very moment — flushes and closes the sinks.
- **Cancellable stop.** `StopAsync(token)`: if the token is cancelled, writing is aborted and the remaining rows are
  counted as dropped.
- **Sink failures** are retried (`SinkRetryCount`); if a batch still fails it is lost *for that sink only*, the
  `SinkError` event is raised and the failure appears in `GetStatus()`. Other sinks keep writing.
- **Full buffer** (sinks cannot keep up): rows are dropped according to `OverflowPolicy` and counted in `RowsDropped`.
  The sensor thread is never blocked.
- **Source read errors** (`PeriodicSensorSource`) are counted and reported through `ReadFailed`; sampling continues.
- **Crash safety:** CSV data is flushed to the operating system after every batch; SQLite commits every batch.
  With `FlushInterval = 1 s`, at most about one second of data is at risk if the process is killed.
- **Lifecycle:** one engine runs one session (`StartAsync` → `StopAsync`); it cannot be restarted. The engine owns and
  disposes the sinks added to it.

```csharp
engine.SinkError += (_, e) => Console.Error.WriteLine($"{e.SinkName} lost {e.RowsLost} rows: {e.Exception.Message}");
```

## Using BufferedWriter and sinks on their own

The writer and the sinks work without the engine, e.g. for data that already has time stamps:

```csharp
var schema = new LogSchema(LogMode.Raw,
    new[] { new ChannelInfo("LoadCell", 100, "N"), new ChannelInfo("Temperature", 10, "°C") },
    sessionStartUtc: SyncClock.Default.UtcNow);

await using var writer = new BufferedWriter(schema, new ILogSink[] { new CsvLogSink("data.csv") });
await writer.StartAsync();

writer.TryEnqueue(LogRow.Raw(SyncClock.Default.UtcNow, channelIndex: 0, value: 1234.5));
writer.TryEnqueue(LogRow.Raw(SyncClock.Default.UtcNow, channelIndex: 1, value: 25.1));

await writer.FlushAsync();   // optional: write now
// DisposeAsync writes the rest and closes the file.
```

Your own destination (a message bus, InfluxDB, a REST API…) only needs `ILogSink`: `OpenAsync(schema)` once,
`WriteBatchAsync(rows)` per batch (write the batch as a unit and do not keep the list), `FlushAsync`, `DisposeAsync`.
Calls to one sink are never concurrent.

## Performance

Measured on a regular Windows 11 development PC (Release build):

| Scenario | Result |
|---|---|
| 1 000 000 rows from 8 threads to CSV **and** SQLite at the same time | written in ~2.3 s (~430 000 rows/s), 0 dropped |
| Cost of enqueuing on a sensor thread | ~1.7 µs per row with 8 threads competing, no I/O |
| 100 Hz / 10 Hz / 1 Hz periodic sources for 10 s | measured 100.0 / 10.0 / 1.0 Hz |

Real test benches produce a few hundred to a few thousand rows per second, so the writer is idle most of the time.

## Platform notes

- **Windows 10 1803+:** periodic sources use a high-resolution waitable timer (~0.5 ms accuracy) without changing the
  system-wide timer resolution. On older Windows versions they fall back to ordinary waits (~15 ms steps).
- **Linux / macOS:** ordinary timed waits are ~1 ms accurate, which is enough for 100 Hz and more.
- The SQLite native library is included through `Microsoft.Data.Sqlite` (Windows, Linux, macOS; x64 and ARM64).

## FAQ

**Can I log raw and aligned data at the same time?**
Yes. Create two engines and register the same sources in both. Let only one engine control the sources:

```csharp
var raw     = new TimeSyncEngine(new TimeSyncOptions { Mode = LogMode.Raw });
var aligned = new TimeSyncEngine(new TimeSyncOptions { Mode = LogMode.Aligned, ManageSourceLifetime = false });
// add the same sources to both, different sinks (or the same SQLite file)
await aligned.StartAsync();   // subscribe first
await raw.StartAsync();       // starts the sources
// ...
await raw.StopAsync();        // stops the sources
await aligned.StopAsync();
```

**Why are some aligned cells empty?** The channel had no value yet at that time, its value was `NaN`, or it was older
than `MaxSampleAge`.

**Which interpolation should I use?** `LastKnownValue` unless you need smooth curves of fast analogue signals. It never
creates values that were not measured.

**How do I open the CSV in Excel with a decimal comma locale (e.g. Turkish, German)?** Use `Delimiter = ';'`, or import
the file with "." as the decimal separator.

**Is the library thread-safe?** Yes. Sources may raise samples from any thread; `GetStatus()`, `FlushAsync()` and
`TryEnqueue()` can be called concurrently.

---

# 🇹🇷 Türkçe

## İçindekiler

1. Hangi sorunu çözüyor?
2. Özellikler
3. Hızlı başlangıç
4. Temel kavramlar
5. Ham mod ve hizalanmış mod
6. Kendi sensörlerinizi bağlamak
7. Çıktı formatları (CSV ve SQLite)
8. Ayarlar
9. GetStatus() ile izleme
10. Kapanış, hatalar ve veri güvenliği
11. BufferedWriter ve çıktıları tek başına kullanmak
12. Performans
13. Platform notları
14. Sık sorulan sorular

## Hangi sorunu çözüyor?

Tipik bir test tezgahında sensörler çok farklı hızlarda çalışır: 100 Hz'de bir load cell, 10 Hz'de bir termokupl,
1 Hz'de bir basınç transmitteri. Bunları doğru loglamak göründüğünden zordur:

- **Zaman damgaları karşılaştırılabilir olmalı.** `DateTime.Now` sistem saati düzeltildiğinde sıçrayabilir ve
  çözünürlüğü kaba olabilir; kanallar birbirinden kayar, hatta "zamanda geri gider".
- **Frekanslar farklı.** Kanalları karşılaştırmak için çoğu zaman her zaman adımında tüm kanalları içeren tek bir satır
  gerekir; ama 1 Hz'lik kanalın saniyede sadece bir değeri vardır.
- **Disk işlemleri veri toplamayı bozmamalı.** Sensör callback'i dosyaya ya da veritabanına yazarsa, yavaş bir disk bir
  sonraki örneği doğrudan geciktirir.
- **Sonda hiçbir şey kaybolmamalı.** Test durduğunda bellekte kalan veri diske ulaşmalıdır.

SensorSyncLogger bu dört sorunu tek bir küçük kütüphanede çözer.

## Özellikler

- **`SyncClock`** – tüm kanallar için Stopwatch tabanlı, geri gitmeyen, mikrosaniye altı çözünürlüklü tek bir UTC zaman ekseni
- **İki log modu** – *Raw/Ham* (her örnek kendi zaman damgasıyla) ve *Aligned/Hizalanmış* (her aralıkta kanal başına bir
  sütun içeren tek satır; "son bilinen değer" ya da doğrusal interpolasyon, düzgün bir zaman ızgarasında)
- **Bloklamayan yapı** – sensör callback'i örneği sadece kilitsiz bir kuyruğa bırakır; tüm disk işini tek bir arka plan yazıcısı yapar
- **Toplu yazma** – belirli sayıda satır birikince ya da belirli süre dolunca (hangisi önce olursa) yazar
- **CSV çıktısı** – sabit bellek kullanan akışlı yazıcı, kültürden bağımsız sayı biçimi, 100 ns hassasiyetli ISO 8601 zaman damgası
- **SQLite çıktısı** – her batch hazır (prepared) komutla tek transaction'da, WAL modu, bir veritabanında birden çok oturum
- **Güvenli kapanış** – durdurulduğunda kabul edilen her satırı yazar, sonra dosyaları kapatır
- **Canlı durum** – gelen örnek sayısı, kanal başına ölçülen frekans, yazılan satır, tampon doluluk oranı, çıktı başına hatalar
- **Hassas periyodik kaynaklar** – `FuncSensorSource` / `PeriodicSensorSource`, normal zamanlayıcının 15,6 ms'de bir
  çalıştığı Windows'ta bile 100 Hz ve üzerinde örnek alır
- **İzolasyon** – hata veren bir çıktı (dolu disk, kilitli veritabanı) diğer çıktıları durdurmaz

## Hızlı başlangıç

```csharp
using SensorSyncLogger.Sinks;
using SensorSyncLogger.Sources;
using SensorSyncLogger.Sync;
using SensorSyncLogger.Writing;

// 1. Sensörler: burada delegate ile okunuyor; olay (event) tabanlı cihazlar için "Kendi sensörlerinizi bağlamak" bölümüne bakın.
var loadCell    = new FuncSensorSource("LoadCell",    100, t => ReadLoadCell(),    "N");
var temperature = new FuncSensorSource("Temperature",  10, t => ReadTemperature(), "°C");
var pressure    = new FuncSensorSource("Pressure",      1, t => ReadPressure(),    "bar");

// 2. Motor: her 100 ms'de bir hizalanmış satır.
await using var engine = new TimeSyncEngine(new TimeSyncOptions
{
    Mode = LogMode.Aligned,
    AlignInterval = TimeSpan.FromMilliseconds(100),
    Interpolation = InterpolationMode.LastKnownValue,
});

engine.AddSource(loadCell)
      .AddSource(temperature)
      .AddSource(pressure)
      .AddSink(new CsvLogSink("test-run.csv"))
      .AddSink(new SqliteLogSink("test-run.db"));

// 3. Çalıştır.
await engine.StartAsync();          // dosyaları açar, sensörleri başlatır
await Task.Delay(TimeSpan.FromSeconds(10));
Console.WriteLine(engine.GetStatus());
await engine.StopAsync();           // sensörleri durdurur, her şeyi yazar, dosyaları kapatır
```

## Temel kavramlar

| Tip | Görevi |
|---|---|
| `SyncClock` | Ortak zaman ekseni. `SyncClock.Default.UtcNow` Stopwatch çözünürlüğünde, geri gitmeyen UTC zaman verir. |
| `ISensorSource` | Her yeni değerde `OnSample` olayını tetikleyen kanal. `ChannelName`, `SampleRateHz` ve isteğe bağlı `Unit` içerir. |
| `SensorSample` | `(DateTime Timestamp, string ChannelName, double Value)` – `double.NaN` geçersiz değer demektir. |
| `TimeSyncEngine` | Kaynaklara abone olur, ham ya da hizalanmış satır üretir ve yazıcıya verir. |
| `BufferedWriter` | Bellek kuyruğu ve satırları toplu halde çıktılara yazan arka plan görevi. |
| `ILogSink` | Hedef: `CsvLogSink`, `SqliteLogSink` ya da kendi yazdığınız. |

Veri akışı:

```
sensör thread'i ──OnSample──► TimeSyncEngine ──TryEnqueue (kilitsiz, disk yok)──► BufferedWriter kuyruğu
                                                                                     │ arka plan görevi
                                                                                     ▼
                                                                      CsvLogSink    SqliteLogSink   (paralel)
```

**Neden özel bir saat?** `SyncClock` oluşturulduğu anda gerçek saati bir kez okur, sonrasında sadece `Stopwatch`
tikleriyle ilerler. Bu yüzden zaman damgaları asla geri gitmez ve mikrosaniye altı çözünürlüğü korur. Saatler süren
çalışmalarda Stopwatch gerçek saatten birkaç milisaniye sapabilir; bir test boyunca önemli olan kanallar arası tutarlı
zamanlama olduğu için saat bilerek yeniden eşitlenmez. Tüm kaynaklar aynı saati (normalde `SyncClock.Default`)
kullanmalıdır; `SensorSourceBase` ve `FuncSensorSource` bunu otomatik yapar.

## Ham mod ve hizalanmış mod

### Ham mod (`LogMode.Raw`, varsayılan)

Her örnek kendi zaman damgasıyla bir satır olur. Hiçbir şey interpolasyonla üretilmez ya da kaybolmaz; her kanal kendi
frekansını korur.

```
timestamp_utc,elapsed_s,channel,value
2026-09-24T15:37:23.6123450Z,0.012,LoadCell,1003.42
2026-09-24T15:37:23.6223561Z,0.022,LoadCell,1006.97
2026-09-24T15:37:23.6251002Z,0.025,Temperature,25.03
```

Orijinal verinin tamamına ihtiyacınız olduğunda kullanın (sinyal analizi, denetim, sonradan yeniden işleme).

### Hizalanmış mod (`LogMode.Aligned`)

Her `AlignInterval`'da (ör. 100 ms) bir satır, her kanal için bir sütun. Satırlar "yuvarlak" zamanlara oturur
(…23.700, …23.800); böylece farklı testlerin dosyaları üst üste konabilir.

```
timestamp_utc,elapsed_s,LoadCell [N],Temperature [°C],Pressure [bar]
2026-09-24T15:37:23.7000000Z,0.0845,1146.21,24.966,6.002
2026-09-24T15:37:23.8000000Z,0.1845,1286.87,24.991,6.002
```

Her hücre nasıl hesaplanır (`Interpolation`):

- **`LastKnownValue`** (varsayılan, örnekle-ve-tut) – ızgara zamanında ya da öncesindeki en yeni örnek. Asla ölçülmemiş
  değer üretmez; yavaş kanallar ve basamaklı sinyaller için en iyisidir. Örnekte 1 Hz'lik basınç, bir sonraki örneğine
  kadar aynı değeri tekrarlar.
- **`Linear`** – ızgara zamanından önceki ve sonraki örnek arasında doğrusal interpolasyon yapar. Hızlı analog
  sinyallerde daha pürüzsüzdür. Sonraki örnek zamanında gelmezse son bilinen değer kullanılır.

Zamanlama ayrıntıları:

- **`AlignmentDelay`** – bir ızgara satırı ancak bu süre geçtikten sonra üretilir; böylece geç gelen örnekler de dahil
  edilir. Varsayılan: `LastKnownValue` için bir aralık; `Linear` için aralık + en yavaş kanalın örnek periyodu (en fazla 10 sn).
- **`MaxSampleAge`** – bir kanalın en yeni değeri ızgara zamanında bu süreden eskiyse, eski değeri sonsuza kadar
  tekrarlamak yerine hücre boş bırakılır (ör. sensör cevap vermeyi bıraktı). Varsayılan: sınırsız.
- En baştaki, hiçbir kanaldan veri gelmemiş satırlar yazılmaz. Sonraki boşluklar (boş hücreler) korunur.
- `StopAsync` çağrıldığında en yeni örneğe kadar kalan satırlar üretilir; dosya verinin bittiği yerde biter.

İki modu aynı anda loglamak için aynı kaynaklarla iki motor kullanabilirsiniz (Sık sorulan sorulara bakın).

## Kendi sensörlerinizi bağlamak

### Olay (event) tabanlı cihazlar (seri port, DAQ callback'i, ağ akışı)

`SensorSourceBase`'den türetin ve değer geldiğinde `Publish` çağırın; değer ortak saatle damgalanır.

```csharp
public sealed class SerialLoadCell : SensorSourceBase
{
    private readonly SerialPort _port;

    public SerialLoadCell(string portName) : base("LoadCell", sampleRateHz: 100, unit: "N")
    {
        _port = new SerialPort(portName, 115200);
        _port.DataReceived += (_, _) =>
        {
            if (double.TryParse(_port.ReadLine(), NumberStyles.Float, CultureInfo.InvariantCulture, out var newton))
                Publish(newton);          // ya da Publish(cihazZamanDamgasiUtc, newton)
        };
    }

    public override Task StartAsync(CancellationToken ct = default) { _port.Open(); return Task.CompletedTask; }
    public override Task StopAsync(CancellationToken ct = default)  { _port.Close(); return Task.CompletedTask; }
}
```

### Sorgulanan cihazlar (Modbus, SCPI, bir fonksiyon çağrısı)

`PeriodicSensorSource`'tan türetip `ReadValue`'yu yazın ya da `FuncSensorSource`'a bir delegate verin. Her kaynak
istenen frekansta kendi thread'inde çalışır.

```csharp
public sealed class ModbusPressure : PeriodicSensorSource
{
    private readonly IMyDevice _device;

    public ModbusPressure(IMyDevice device) : base("Pressure", sampleRateHz: 10, unit: "bar") => _device = device;

    protected override double ReadValue(DateTime timestampUtc) => _device.ReadPressure();
}
```

Periyodik kaynakların davranışı:

- Tikler mutlak bir ızgaraya göre planlanır (başlangıç + n × periyot); frekans zamanla kaymaz.
- Döngü `MaxLagPeriods` (varsayılan 5) periyottan fazla geride kalırsa, kaçırılan tikler toplu halde üretilmek yerine
  atlanır ve `MissedTicks` içinde sayılır.
- `ReadValue` hata fırlatırsa o örnek atlanır, `ReadErrors` artar, `ReadFailed` olayı tetiklenir ve örnekleme devam eder.
- `ReadValue` bir periyottan kısa sürede dönmelidir.

### `ISensorSource`'u doğrudan uygulamak

`ChannelName`, `SampleRateHz`, isteğe bağlı `Unit`, `OnSample` olayı ve `StartAsync`/`StopAsync` sağlayın. Tüm kanallar
aynı zaman eksenini paylaşsın diye zaman damgalarını `SyncClock.Default.UtcNow`'dan alın. `OnSample` herhangi bir
thread'den tetiklenebilir ve hızlı dönmelidir (motor onu asla bloklamaz).

## Çıktı formatları (CSV ve SQLite)

### CSV (`CsvLogSink`)

```csharp
new CsvLogSink("test.csv", new CsvLogSinkOptions
{
    Delimiter = ',',               // ondalık ayırıcısı virgül olan Excel'ler (Türkçe) için ';'
    Append = false,                // true: var olan dosyanın sonuna ekle (başlık sadece yeni dosyada)
    IncludeElapsedSeconds = true,  // elapsed_s sütunu; grafik çizmek için pratik
    IncludeUnitsInHeader = true,   // hizalanmış modda "LoadCell [N]"
    FlushToDisk = false,           // true: her flush'ta veriyi fiziksel diske zorla (daha yavaş)
});
```

- BOM'suz UTF-8, ondalık ayırıcı `.`, gerektiğinde alanlar RFC 4180'e göre tırnaklanır.
- Zaman damgası: 7 ondalık haneli ISO 8601 UTC, ör. `2026-09-24T15:37:23.7000000Z`.
- `NaN` (değer yok) boş alan olarak yazılır.
- Bellek kullanımı dosya büyüdükçe artmaz: her batch havuzdan alınan bir tamponda biçimlendirilip tek seferde yazılır.

### SQLite (`SqliteLogSink`)

```csharp
new SqliteLogSink("test.db", new SqliteLogSinkOptions
{
    RawTableName = "samples",
    AlignedTableName = "aligned_samples",
    CreateIndexes = true,                     // (session_id, ts_unix_us) indeksi
    SynchronousFull = false,                  // true: elektrik kesintisinde de güvenli, daha yavaş
    BusyTimeout = TimeSpan.FromSeconds(5),    // başka bağlantı yazıyorsa bekleme süresi
});
```

Tablolar (otomatik oluşturulur; birden çok test aynı dosyayı paylaşabilir, her test bir *oturumdur*):

| Tablo | Sütunlar |
|---|---|
| `sessions` | `session_id`, `started_utc`, `mode`, `align_interval_ms`, `machine_name` |
| `channels` | `session_id`, `channel_index`, `name`, `unit`, `sample_rate_hz` |
| `samples` (ham) | `session_id`, `ts_unix_us`, `channel_index`, `value` |
| `samples_view` (ham, okunabilir) | `session_id`, `timestamp_utc`, `channel`, `unit`, `value` |
| `aligned_samples` (hizalanmış) | `session_id`, `ts_unix_us`, her kanal adı için bir `REAL` sütun |

- `ts_unix_us` = 1970-01-01 UTC'den beri geçen mikrosaniye (tam sayı). `NaN` değerler `NULL` olarak saklanır.
- Sonraki bir oturumda yeni kanallar varsa `aligned_samples` tablosuna sütunlar otomatik eklenir.
- WAL modu: loglama sürerken veritabanını açıp sorgulayabilirsiniz.

İşe yarar sorgular:

```sql
-- tüm testler
SELECT * FROM sessions ORDER BY session_id DESC;

-- bir kanalın ham verisi, okunabilir zamanla
SELECT timestamp_utc, value FROM samples_view
WHERE session_id = 1 AND channel = 'LoadCell' ORDER BY timestamp_utc;

-- hizalanmış veri, okunabilir zaman sütunuyla
SELECT strftime('%Y-%m-%dT%H:%M:%f', ts_unix_us / 1e6, 'unixepoch') AS zaman, LoadCell, Temperature, Pressure
FROM aligned_samples WHERE session_id = 2 ORDER BY ts_unix_us;
```

## Ayarlar

### `TimeSyncOptions`

| Ayar | Varsayılan | Anlamı |
|---|---|---|
| `Mode` | `Raw` | `Raw` (ham) ya da `Aligned` (hizalanmış) |
| `AlignInterval` | 100 ms | Hizalanmış modda ızgara aralığı (≥ 1 ms) |
| `Interpolation` | `LastKnownValue` | `LastKnownValue` ya da `Linear` |
| `AlignmentDelay` | otomatik | Izgara satırı üretilmeden önce geç örnekler için bekleme |
| `MaxSampleAge` | sınırsız | Bundan eski değerler boş hücre olur |
| `ManageSourceLifetime` | `true` | Motor kaynakları başlatır/durdurur; kaynakları motorlar arasında paylaşmak için `false` |
| `Writer` | aşağıya bakın | Tampon ayarları |
| `Clock` | `SyncClock.Default` | Kaynakların damgaladığı saatle aynı olmalı |

### `BufferedWriterOptions` (`TimeSyncOptions.Writer`)

| Ayar | Varsayılan | Anlamı |
|---|---|---|
| `BatchSize` | 1000 | Yazma başına satır; bu kadar satır birikince yazma hemen başlar |
| `FlushInterval` | 1 sn | Bir satırın bellekte kalabileceği en uzun süre |
| `Capacity` | 500 000 | En fazla tamponlanan satır (satır başı ≈ 40 bayt) |
| `OverflowPolicy` | `DropNewest` | Tampon doluysa: yeni satırı reddet (`DropNewest`) ya da en eskiyi at (`DropOldest`) |
| `SinkRetryCount` | 2 | Başarısız batch'in çıktı başına tekrar deneme sayısı |
| `SinkRetryDelay` | 100 ms | Denemeler arası bekleme |

## GetStatus() ile izleme

```csharp
SyncStatus durum = engine.GetStatus();

Console.WriteLine($"{durum.State}, çalışma süresi {durum.Uptime}");
Console.WriteLine($"örnek {durum.SamplesReceived}, satır {durum.RowsProduced}");
Console.WriteLine($"yazılan {durum.Buffer!.RowsWritten}, düşen {durum.Buffer.RowsDropped}, tampon {durum.Buffer.FillRatio:P1}");

foreach (var kanal in durum.Channels)
    Console.WriteLine($"{kanal.Name}: {kanal.MeasuredRateHz:0.0} Hz (nominal {kanal.NominalRateHz}), son {kanal.LastValue} {kanal.Unit}");

foreach (var cikti in durum.Buffer.Sinks)
    Console.WriteLine($"{cikti.Name}: {cikti.RowsWritten} satır, {cikti.FailedBatches} başarısız batch");
```

`durum.ToString()` bunların hepsini kısa bir biçimde yazdırır. `GetStatus()` ucuz ve thread-safe'tir; bir arayüz
zamanlayıcısından her saniye çağırmak sorun olmaz.

## Kapanış, hatalar ve veri güvenliği

- **Durdurma güvenlidir.** `StopAsync` (ya da `await using`) kaynakları durdurur, son hizalanmış satırları üretir,
  kabul edilmiş her satırı — tam o anda kuyruğa yazılmakta olanlar dahil — yazar, çıktıları flush edip kapatır.
- **İptal edilebilir durdurma.** `StopAsync(token)`: token iptal edilirse yazma yarıda kesilir, kalan satırlar düşmüş sayılır.
- **Çıktı hataları** tekrar denenir (`SinkRetryCount`); batch yine başarısız olursa *sadece o çıktı için* kaybolur,
  `SinkError` olayı tetiklenir ve hata `GetStatus()`'ta görünür. Diğer çıktılar yazmaya devam eder.
- **Dolu tampon** (çıktılar yetişemiyor): satırlar `OverflowPolicy`'ye göre düşürülür ve `RowsDropped` içinde sayılır.
  Sensör thread'i asla bloklanmaz.
- **Kaynak okuma hataları** (`PeriodicSensorSource`) sayılır ve `ReadFailed` ile bildirilir; örnekleme devam eder.
- **Çökme güvenliği:** CSV verisi her batch'ten sonra işletim sistemine aktarılır; SQLite her batch'i commit eder.
  `FlushInterval = 1 sn` ile süreç aniden kapansa en fazla yaklaşık bir saniyelik veri risk altındadır.
- **Yaşam döngüsü:** bir motor bir oturum çalıştırır (`StartAsync` → `StopAsync`); yeniden başlatılamaz. Motora eklenen
  çıktıların sahibi motordur ve onları kapatır.

```csharp
engine.SinkError += (_, e) => Console.Error.WriteLine($"{e.SinkName} {e.RowsLost} satır kaybetti: {e.Exception.Message}");
```

## BufferedWriter ve çıktıları tek başına kullanmak

Yazıcı ve çıktılar motor olmadan da çalışır; ör. zaten zaman damgası olan veriler için:

```csharp
var schema = new LogSchema(LogMode.Raw,
    new[] { new ChannelInfo("LoadCell", 100, "N"), new ChannelInfo("Temperature", 10, "°C") },
    sessionStartUtc: SyncClock.Default.UtcNow);

await using var writer = new BufferedWriter(schema, new ILogSink[] { new CsvLogSink("veri.csv") });
await writer.StartAsync();

writer.TryEnqueue(LogRow.Raw(SyncClock.Default.UtcNow, channelIndex: 0, value: 1234.5));
writer.TryEnqueue(LogRow.Raw(SyncClock.Default.UtcNow, channelIndex: 1, value: 25.1));

await writer.FlushAsync();   // isteğe bağlı: hemen yaz
// DisposeAsync kalanları yazar ve dosyayı kapatır.
```

Kendi hedefiniz (mesaj kuyruğu, InfluxDB, bir REST API…) için sadece `ILogSink` yeterlidir: bir kez
`OpenAsync(schema)`, her batch için `WriteBatchAsync(rows)` (batch'i bir bütün olarak yazın ve listeyi saklamayın),
`FlushAsync`, `DisposeAsync`. Bir çıktıya yapılan çağrılar asla eşzamanlı değildir.

## Performans

Sıradan bir Windows 11 geliştirme bilgisayarında ölçüldü (Release derleme):

| Senaryo | Sonuç |
|---|---|
| 8 thread'den 1 000 000 satır, aynı anda CSV **ve** SQLite'a | ~2,3 sn'de yazıldı (~430 000 satır/sn), 0 kayıp |
| Sensör thread'inde kuyruğa yazmanın maliyeti | 8 thread yarışırken satır başına ~1,7 µs, disk işlemi yok |
| 10 sn boyunca 100 Hz / 10 Hz / 1 Hz periyodik kaynaklar | ölçülen 100,0 / 10,0 / 1,0 Hz |

Gerçek test tezgahları saniyede birkaç yüz ile birkaç bin satır üretir; yazıcı zamanının çoğunu boşta geçirir.

## Platform notları

- **Windows 10 1803+:** periyodik kaynaklar sistem genelindeki zamanlayıcı ayarını değiştirmeden yüksek çözünürlüklü
  bir bekleme zamanlayıcısı kullanır (~0,5 ms hassasiyet). Daha eski Windows'larda normal beklemeye (~15 ms adım) döner.
- **Linux / macOS:** normal zamanlı beklemeler ~1 ms hassasiyettedir; 100 Hz ve üzeri için yeterlidir.
- SQLite'ın yerel kütüphanesi `Microsoft.Data.Sqlite` ile birlikte gelir (Windows, Linux, macOS; x64 ve ARM64).

## Sık sorulan sorular

**Ham ve hizalanmış veriyi aynı anda loglayabilir miyim?**
Evet. İki motor oluşturup aynı kaynakları ikisine de ekleyin. Kaynakları sadece bir motor yönetsin:

```csharp
var ham        = new TimeSyncEngine(new TimeSyncOptions { Mode = LogMode.Raw });
var hizalanmis = new TimeSyncEngine(new TimeSyncOptions { Mode = LogMode.Aligned, ManageSourceLifetime = false });
// aynı kaynakları ikisine de ekleyin; çıktılar farklı olabilir (ya da aynı SQLite dosyası)
await hizalanmis.StartAsync();   // önce abone olsun
await ham.StartAsync();          // kaynakları başlatır
// ...
await ham.StopAsync();           // kaynakları durdurur
await hizalanmis.StopAsync();
```

**Hizalanmış bazı hücreler neden boş?** O anda kanalın henüz değeri yoktu, değeri `NaN` idi ya da `MaxSampleAge`'den eskiydi.

**Hangi interpolasyonu kullanmalıyım?** Hızlı analog sinyallerin pürüzsüz eğrisine ihtiyacınız yoksa `LastKnownValue`.
Ölçülmemiş hiçbir değer üretmez.

**CSV'yi Türkçe Excel'de nasıl açarım?** `Delimiter = ';'` kullanın ya da dosyayı içe aktarırken ondalık ayırıcı olarak
"." seçin.

**Kütüphane thread-safe mi?** Evet. Kaynaklar örnekleri herhangi bir thread'den gönderebilir; `GetStatus()`,
`FlushAsync()` ve `TryEnqueue()` eşzamanlı çağrılabilir.

---

MIT License © Semih Bener
