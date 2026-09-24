namespace SensorSyncLogger.Sources;

/// <summary>One measurement of one channel.</summary>
/// <param name="Timestamp">Acquisition time in UTC, ideally taken from <see cref="Timing.SyncClock"/>.</param>
/// <param name="ChannelName">Name of the channel that produced the sample.</param>
/// <param name="Value">Measured value. <see cref="double.NaN"/> marks an invalid measurement.</param>
public readonly record struct SensorSample(DateTime Timestamp, string ChannelName, double Value);
