using SensorSyncLogger.Timing;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Sync;

/// <summary>How a channel's value at a grid time is derived in <see cref="LogMode.Aligned"/> mode.</summary>
public enum InterpolationMode
{
    /// <summary>
    /// Sample-and-hold: the latest sample at or before the grid time. Never invents values; the natural choice
    /// for slow channels and for anything that changes in steps.
    /// </summary>
    LastKnownValue,

    /// <summary>
    /// Linear interpolation between the samples before and after the grid time. Smoother for fast analogue signals;
    /// requires waiting for the next sample (see <see cref="TimeSyncOptions.AlignmentDelay"/>). When the next sample has
    /// not arrived in time, the last known value is used.
    /// </summary>
    Linear,
}

/// <summary>Configuration of <see cref="TimeSyncEngine"/>.</summary>
public sealed class TimeSyncOptions
{
    /// <summary>Raw rows or time-aligned rows. Defaults to <see cref="LogMode.Raw"/>.</summary>
    public LogMode Mode { get; set; } = LogMode.Raw;

    /// <summary>Grid interval in aligned mode. Defaults to 100 ms.</summary>
    public TimeSpan AlignInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Value derivation in aligned mode. Defaults to <see cref="InterpolationMode.LastKnownValue"/>.</summary>
    public InterpolationMode Interpolation { get; set; } = InterpolationMode.LastKnownValue;

    /// <summary>
    /// How long a grid point waits for late samples before its row is produced. <c>null</c> (default) picks one
    /// <see cref="AlignInterval"/> for <see cref="InterpolationMode.LastKnownValue"/>, and for
    /// <see cref="InterpolationMode.Linear"/> the interval plus the slowest channel's sample period (max 10 s).
    /// </summary>
    public TimeSpan? AlignmentDelay { get; set; }

    /// <summary>
    /// In aligned mode, values older than this at a grid time are written as empty (NaN) instead of being held
    /// forever — e.g. when a sensor stops responding. <c>null</c> (default) holds indefinitely.
    /// </summary>
    public TimeSpan? MaxSampleAge { get; set; }

    /// <summary>
    /// When <c>true</c> (default) the engine starts and stops the registered sources. Set to <c>false</c> when the
    /// sources are shared with another engine or managed by the application.
    /// </summary>
    public bool ManageSourceLifetime { get; set; } = true;

    /// <summary>Buffering and flushing behaviour.</summary>
    public BufferedWriterOptions Writer { get; set; } = new();

    /// <summary>Clock for "now" in aligned mode; must be the clock the sources stamp with. Defaults to <see cref="SyncClock.Default"/>.</summary>
    public SyncClock? Clock { get; set; }

    internal void Validate()
    {
        if (!Enum.IsDefined(Mode))
            throw new ArgumentOutOfRangeException(nameof(Mode), Mode, "Unknown log mode.");
        if (!Enum.IsDefined(Interpolation))
            throw new ArgumentOutOfRangeException(nameof(Interpolation), Interpolation, "Unknown interpolation mode.");
        if (Mode == LogMode.Aligned && AlignInterval < TimeSpan.FromMilliseconds(1))
            throw new ArgumentOutOfRangeException(nameof(AlignInterval), AlignInterval, "The alignment interval must be at least 1 ms.");
        if (AlignmentDelay is { } delay && delay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(AlignmentDelay), delay, "The alignment delay must not be negative.");
        if (MaxSampleAge is { } age && age <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(MaxSampleAge), age, "The maximum sample age must be positive.");
        ArgumentNullException.ThrowIfNull(Writer, nameof(Writer));
        Writer.Validate();
    }
}
