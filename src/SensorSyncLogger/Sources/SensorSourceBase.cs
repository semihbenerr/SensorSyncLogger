using SensorSyncLogger.Timing;

namespace SensorSyncLogger.Sources;

/// <summary>
/// Convenience base class for <see cref="ISensorSource"/> implementations: stores the channel metadata and
/// publishes samples stamped with a shared <see cref="SyncClock"/>.
/// </summary>
public abstract class SensorSourceBase : ISensorSource
{
    /// <summary>Creates the source.</summary>
    /// <param name="channelName">Unique channel name.</param>
    /// <param name="sampleRateHz">Nominal sampling frequency (&gt; 0).</param>
    /// <param name="unit">Optional engineering unit.</param>
    /// <param name="clock">Clock used for timestamps; <see cref="SyncClock.Default"/> when <c>null</c>.</param>
    protected SensorSourceBase(string channelName, double sampleRateHz, string? unit = null, SyncClock? clock = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(channelName);
        if (!double.IsFinite(sampleRateHz) || sampleRateHz <= 0)
            throw new ArgumentOutOfRangeException(nameof(sampleRateHz), sampleRateHz, "Sample rate must be a positive, finite number.");

        ChannelName = channelName;
        SampleRateHz = sampleRateHz;
        Unit = unit;
        Clock = clock ?? SyncClock.Default;
    }

    /// <inheritdoc />
    public string ChannelName { get; }

    /// <inheritdoc />
    public double SampleRateHz { get; }

    /// <inheritdoc />
    public string? Unit { get; }

    /// <summary>Clock used to stamp samples.</summary>
    public SyncClock Clock { get; }

    /// <inheritdoc />
    public event Action<SensorSample>? OnSample;

    /// <inheritdoc />
    public virtual Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public virtual Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>Publishes a value stamped with the current <see cref="Clock"/> time.</summary>
    protected void Publish(double value) => Publish(Clock.UtcNow, value);

    /// <summary>Publishes a value with an explicit UTC timestamp (e.g. a device-side acquisition time).</summary>
    protected void Publish(DateTime timestampUtc, double value) =>
        OnSample?.Invoke(new SensorSample(timestampUtc, ChannelName, value));

    /// <inheritdoc />
    public override string ToString() => $"{ChannelName} ({SampleRateHz:0.###} Hz{(Unit is null ? "" : ", " + Unit)})";
}
