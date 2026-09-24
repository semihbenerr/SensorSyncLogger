namespace SensorSyncLogger.Writing;

/// <summary>Shape of the rows produced by a session.</summary>
public enum LogMode
{
    /// <summary>One row per sample: timestamp, channel, value — each channel at its own rate and timestamps.</summary>
    Raw,

    /// <summary>One row per alignment interval with a column per channel, all on a common time grid.</summary>
    Aligned,
}

/// <summary>Metadata of one logged channel.</summary>
/// <param name="Name">Channel name.</param>
/// <param name="SampleRateHz">Nominal sampling frequency.</param>
/// <param name="Unit">Optional engineering unit.</param>
public sealed record ChannelInfo(string Name, double SampleRateHz, string? Unit = null);

/// <summary>Describes the rows a sink will receive; passed to <see cref="ILogSink.OpenAsync"/>.</summary>
public sealed class LogSchema
{
    /// <summary>Creates a schema.</summary>
    /// <param name="mode">Row shape.</param>
    /// <param name="channels">Channels in column order (raw rows refer to them by index).</param>
    /// <param name="sessionStartUtc">Start of the session (used for elapsed-time columns).</param>
    /// <param name="alignInterval">Grid interval for <see cref="LogMode.Aligned"/>; <c>null</c> for raw mode.</param>
    public LogSchema(LogMode mode, IReadOnlyList<ChannelInfo> channels, DateTime sessionStartUtc, TimeSpan? alignInterval = null)
    {
        ArgumentNullException.ThrowIfNull(channels);
        if (channels.Count == 0)
            throw new ArgumentException("At least one channel is required.", nameof(channels));

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var channel in channels)
        {
            ArgumentNullException.ThrowIfNull(channel, nameof(channels));
            if (!names.Add(channel.Name))
                throw new ArgumentException($"Duplicate channel name '{channel.Name}' (names are case-insensitive).", nameof(channels));
        }

        if (mode == LogMode.Aligned && (alignInterval is null || alignInterval <= TimeSpan.Zero))
            throw new ArgumentException("Aligned mode requires a positive alignment interval.", nameof(alignInterval));

        Mode = mode;
        Channels = channels.ToArray();
        SessionStartUtc = DateTime.SpecifyKind(sessionStartUtc, DateTimeKind.Utc);
        AlignInterval = mode == LogMode.Aligned ? alignInterval : null;
    }

    /// <summary>Row shape.</summary>
    public LogMode Mode { get; }

    /// <summary>Channels in column order.</summary>
    public IReadOnlyList<ChannelInfo> Channels { get; }

    /// <summary>Start of the session in UTC.</summary>
    public DateTime SessionStartUtc { get; }

    /// <summary>Grid interval in aligned mode.</summary>
    public TimeSpan? AlignInterval { get; }
}

/// <summary>
/// One row to log. Raw rows carry a channel index and a value; aligned rows carry one value per channel.
/// <see cref="double.NaN"/> means "no valid value" and is written as an empty CSV field / SQL NULL.
/// </summary>
public readonly struct LogRow
{
    private LogRow(DateTime timestamp, int channelIndex, double value, double[]? values)
    {
        Timestamp = timestamp;
        ChannelIndex = channelIndex;
        Value = value;
        Values = values;
    }

    /// <summary>UTC timestamp of the row.</summary>
    public DateTime Timestamp { get; }

    /// <summary>Index into <see cref="LogSchema.Channels"/> (raw rows; -1 for aligned rows).</summary>
    public int ChannelIndex { get; }

    /// <summary>Sample value (raw rows).</summary>
    public double Value { get; }

    /// <summary>One value per channel in schema order (aligned rows; <c>null</c> for raw rows).</summary>
    public double[]? Values { get; }

    /// <summary><c>true</c> for aligned rows.</summary>
    public bool IsAligned => Values is not null;

    /// <summary>Creates a raw row.</summary>
    public static LogRow Raw(DateTime timestampUtc, int channelIndex, double value)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(channelIndex);
        return new LogRow(timestampUtc, channelIndex, value, null);
    }

    /// <summary>Creates an aligned row. The array is taken over, not copied.</summary>
    public static LogRow Aligned(DateTime timestampUtc, double[] values)
    {
        ArgumentNullException.ThrowIfNull(values);
        return new LogRow(timestampUtc, -1, double.NaN, values);
    }
}
