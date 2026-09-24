namespace SensorSyncLogger.Writing;

/// <summary>What happens when the buffer is full because the sinks cannot keep up.</summary>
public enum OverflowPolicy
{
    /// <summary>Reject the new row (the oldest data is kept).</summary>
    DropNewest,

    /// <summary>Discard the oldest buffered row to make room (the most recent data is kept).</summary>
    DropOldest,
}

/// <summary>Tuning of <see cref="BufferedWriter"/>.</summary>
public sealed class BufferedWriterOptions
{
    /// <summary>Rows per batch; a flush starts as soon as this many rows are waiting. Defaults to 1000.</summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>Maximum time rows wait in memory before being written, even if the batch is not full. Defaults to 1 s.</summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Maximum number of buffered rows (≈ 40 bytes each). Defaults to 500 000.</summary>
    public int Capacity { get; set; } = 500_000;

    /// <summary>Behaviour when <see cref="Capacity"/> is reached. Defaults to <see cref="OverflowPolicy.DropNewest"/>.</summary>
    public OverflowPolicy OverflowPolicy { get; set; } = OverflowPolicy.DropNewest;

    /// <summary>How many times a failed batch is retried per sink before it is counted as lost. Defaults to 2.</summary>
    public int SinkRetryCount { get; set; } = 2;

    /// <summary>Pause between sink retries. Defaults to 100 ms.</summary>
    public TimeSpan SinkRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    internal void Validate()
    {
        if (BatchSize < 1)
            throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "Batch size must be at least 1.");
        if (FlushInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(FlushInterval), FlushInterval, "Flush interval must be positive.");
        if (Capacity < BatchSize)
            throw new ArgumentOutOfRangeException(nameof(Capacity), Capacity, "Capacity must be at least the batch size.");
        if (!Enum.IsDefined(OverflowPolicy))
            throw new ArgumentOutOfRangeException(nameof(OverflowPolicy), OverflowPolicy, "Unknown overflow policy.");
        if (SinkRetryCount < 0)
            throw new ArgumentOutOfRangeException(nameof(SinkRetryCount), SinkRetryCount, "Retry count must not be negative.");
        if (SinkRetryDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SinkRetryDelay), SinkRetryDelay, "Retry delay must not be negative.");
    }

    internal BufferedWriterOptions Clone() => (BufferedWriterOptions)MemberwiseClone();
}
