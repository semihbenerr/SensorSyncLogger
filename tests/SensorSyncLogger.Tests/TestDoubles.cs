using SensorSyncLogger.Sources;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Tests;

/// <summary>Sink that keeps rows in memory; can be made to fail or to block inside a write.</summary>
internal sealed class MemorySink : ILogSink
{
    private readonly object _gate = new();
    private readonly List<LogRow> _rows = new();
    private readonly List<int> _batchSizes = new();

    public MemorySink(string name = "memory") => Name = name;

    public string Name { get; }
    public LogSchema? Schema { get; private set; }
    public bool AlwaysFail { get; set; }
    public int FlushCount { get; private set; }
    public bool Disposed { get; private set; }

    /// <summary>When set, the next write signals <see cref="WriteEntered"/> and waits for this gate.</summary>
    public SemaphoreSlim? BlockNextWrite { get; set; }
    public TaskCompletionSource WriteEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IReadOnlyList<LogRow> Rows
    {
        get
        {
            lock (_gate)
                return _rows.ToArray();
        }
    }

    public IReadOnlyList<int> BatchSizes
    {
        get
        {
            lock (_gate)
                return _batchSizes.ToArray();
        }
    }

    public ValueTask OpenAsync(LogSchema schema, CancellationToken cancellationToken = default)
    {
        Schema = schema;
        return ValueTask.CompletedTask;
    }

    public async ValueTask WriteBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken cancellationToken = default)
    {
        if (BlockNextWrite is { } gate)
        {
            BlockNextWrite = null;
            WriteEntered.TrySetResult();
            await gate.WaitAsync(cancellationToken);
        }

        if (AlwaysFail)
            throw new IOException("disk full");

        lock (_gate)
        {
            _rows.AddRange(rows);
            _batchSizes.Add(rows.Count);
        }
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        FlushCount++;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

/// <summary>Source whose samples are pushed by the test.</summary>
internal sealed class ManualSource : SensorSourceBase
{
    public ManualSource(string name, double rateHz = 10, string? unit = null) : base(name, rateHz, unit) { }

    public int StartCount { get; private set; }
    public int StopCount { get; private set; }

    public void Emit(DateTime timestampUtc, double value) => Publish(timestampUtc, value);

    public override Task StartAsync(CancellationToken cancellationToken = default)
    {
        StartCount++;
        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken = default)
    {
        StopCount++;
        return Task.CompletedTask;
    }
}

internal static class TestSchema
{
    public static readonly DateTime Start = new(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);

    public static LogSchema Raw(params string[] channels) =>
        new(LogMode.Raw, channels.Select(c => new ChannelInfo(c, 10)).ToArray(), Start);

    public static LogSchema Aligned(params string[] channels) =>
        new(LogMode.Aligned, channels.Select(c => new ChannelInfo(c, 10, "u")).ToArray(), Start, TimeSpan.FromMilliseconds(100));
}
