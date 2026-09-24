using System.Globalization;
using System.Text;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Sync;

/// <summary>Lifecycle of a <see cref="TimeSyncEngine"/>.</summary>
public enum EngineState
{
    /// <summary>Sources and sinks can be added.</summary>
    Created,

    /// <summary>Opening sinks and starting sources.</summary>
    Starting,

    /// <summary>Logging.</summary>
    Running,

    /// <summary>Stopping sources and writing the remaining data.</summary>
    Stopping,

    /// <summary>Finished; the engine cannot be restarted.</summary>
    Stopped,
}

/// <summary>Live counters of one channel.</summary>
/// <param name="Name">Channel name.</param>
/// <param name="Unit">Engineering unit.</param>
/// <param name="NominalRateHz">Declared sample rate.</param>
/// <param name="SamplesReceived">Samples received from the source.</param>
/// <param name="MeasuredRateHz">Samples per second since the first sample (0 until two samples were received).</param>
/// <param name="LastValue">Most recent value (NaN if none).</param>
/// <param name="LastTimestampUtc">Timestamp of the most recent sample.</param>
public sealed record ChannelStatus(string Name, string? Unit, double NominalRateHz, long SamplesReceived,
    double MeasuredRateHz, double LastValue, DateTime? LastTimestampUtc);

/// <summary>Snapshot returned by <see cref="TimeSyncEngine.GetStatus"/>.</summary>
public sealed record SyncStatus
{
    /// <summary>Engine state.</summary>
    public required EngineState State { get; init; }

    /// <summary>Raw or aligned.</summary>
    public required LogMode Mode { get; init; }

    /// <summary>Session start (UTC), once started.</summary>
    public required DateTime? StartedAtUtc { get; init; }

    /// <summary>Time since start.</summary>
    public required TimeSpan Uptime { get; init; }

    /// <summary>Samples received from all sources.</summary>
    public required long SamplesReceived { get; init; }

    /// <summary>Rows handed to the writer (raw: one per sample; aligned: one per grid interval).</summary>
    public required long RowsProduced { get; init; }

    /// <summary>Buffer and sink counters.</summary>
    public required BufferStatus? Buffer { get; init; }

    /// <summary>Per-channel counters in column order.</summary>
    public required IReadOnlyList<ChannelStatus> Channels { get; init; }

    /// <inheritdoc />
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append(CultureInfo.InvariantCulture,
            $"{State} {Mode} {Uptime:hh\\:mm\\:ss\\.f} | samples {SamplesReceived}, rows {RowsProduced}");
        if (Buffer is not null)
            sb.Append(" | ").Append(Buffer);
        foreach (var channel in Channels)
        {
            sb.AppendLine();
            sb.Append(CultureInfo.InvariantCulture,
                $"  {channel.Name,-16} {channel.SamplesReceived,8} samples  {channel.MeasuredRateHz,8:0.0} Hz (nominal {channel.NominalRateHz:0.###})  last {channel.LastValue:0.###} {channel.Unit}");
        }

        return sb.ToString();
    }
}
