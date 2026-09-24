namespace SensorSyncLogger.Sources;

/// <summary>A sensor channel that pushes samples as they are acquired.</summary>
/// <remarks>
/// <para><see cref="OnSample"/> may be raised from any thread. Handlers must return quickly;
/// <see cref="Sync.TimeSyncEngine"/> only enqueues the sample and never performs I/O on the caller's thread.</para>
/// <para>Timestamps should come from <see cref="Timing.SyncClock.Default"/> (or the engine's clock) so that all
/// channels share one time base. <see cref="SensorSourceBase"/> does this automatically.</para>
/// </remarks>
public interface ISensorSource
{
    /// <summary>Unique channel name, used as column/channel name in the logs.</summary>
    string ChannelName { get; }

    /// <summary>Nominal sampling frequency in Hz (used for sizing buffers and for status reports).</summary>
    double SampleRateHz { get; }

    /// <summary>Engineering unit, e.g. <c>"N"</c>, <c>"°C"</c>, <c>"bar"</c>. Optional.</summary>
    string? Unit => null;

    /// <summary>Raised for every new sample.</summary>
    event Action<SensorSample>? OnSample;

    /// <summary>Starts producing samples. Sources that are driven externally may complete immediately.</summary>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>Stops producing samples. After completion no further <see cref="OnSample"/> events are raised.</summary>
    Task StopAsync(CancellationToken cancellationToken = default);
}
