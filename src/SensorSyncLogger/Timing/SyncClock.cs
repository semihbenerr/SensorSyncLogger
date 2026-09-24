namespace SensorSyncLogger.Timing;

/// <summary>
/// High-resolution, monotonic UTC clock shared by all sensors of a session.
/// </summary>
/// <remarks>
/// <para><see cref="DateTime.UtcNow"/> is not suitable for time-aligning sensors: it can jump when the system clock
/// is corrected (NTP synchronisation, manual changes) and, depending on the platform, its effective resolution
/// may be coarse. <see cref="SyncClock"/> reads the wall clock once (the anchor)
/// and afterwards advances purely by <see cref="System.Diagnostics.Stopwatch"/> ticks, so timestamps never go
/// backwards and have sub-microsecond resolution.</para>
/// <para>Over many hours the Stopwatch may drift a few milliseconds from the wall clock. Within one logging session
/// consistent relative timing matters more than absolute accuracy, so the clock is deliberately never re-anchored.</para>
/// <para>All sources and engines should use the same instance (by default <see cref="Default"/>) so that their
/// timestamps are comparable.</para>
/// </remarks>
public sealed class SyncClock
{
    private readonly DateTime _anchorUtc;
    private readonly long _anchorTimestamp;

    /// <summary>Creates a clock anchored to the current time of <paramref name="timeProvider"/>.</summary>
    public SyncClock(TimeProvider? timeProvider = null)
    {
        TimeProvider = timeProvider ?? TimeProvider.System;
        _anchorUtc = TimeProvider.GetUtcNow().UtcDateTime;
        _anchorTimestamp = TimeProvider.GetTimestamp();
    }

    /// <summary>Process-wide clock used by default by sources and engines.</summary>
    public static SyncClock Default { get; } = new();

    /// <summary>Underlying time provider (replaceable in tests).</summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>UTC time of the anchor (when the clock was created).</summary>
    public DateTime AnchorUtc => _anchorUtc;

    /// <summary>Current UTC time with Stopwatch resolution. Never goes backwards.</summary>
    public DateTime UtcNow => _anchorUtc + TimeProvider.GetElapsedTime(_anchorTimestamp);

    /// <summary>Raw high-resolution timestamp (see <see cref="TimeProvider.GetTimestamp"/>).</summary>
    public long GetTimestamp() => TimeProvider.GetTimestamp();

    /// <summary>Converts a raw timestamp obtained from <see cref="GetTimestamp"/> to UTC.</summary>
    public DateTime ToUtc(long timestamp) => _anchorUtc + TimeProvider.GetElapsedTime(_anchorTimestamp, timestamp);
}
