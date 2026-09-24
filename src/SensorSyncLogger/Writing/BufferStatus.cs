using System.Globalization;
using System.Text;

namespace SensorSyncLogger.Writing;

/// <summary>Per-sink counters.</summary>
/// <param name="Name">Sink name.</param>
/// <param name="RowsWritten">Rows successfully written.</param>
/// <param name="FailedBatches">Batches that failed after all retries.</param>
/// <param name="RowsLost">Rows of failed batches.</param>
/// <param name="LastError">Message of the last failure, if any.</param>
public sealed record SinkStatus(string Name, long RowsWritten, long FailedBatches, long RowsLost, string? LastError);

/// <summary>Snapshot of <see cref="BufferedWriter"/> state.</summary>
public sealed record BufferStatus
{
    /// <summary><c>true</c> while rows are accepted.</summary>
    public required bool IsRunning { get; init; }

    /// <summary>Rows accepted into the buffer.</summary>
    public required long RowsEnqueued { get; init; }

    /// <summary>Rows written successfully to every sink.</summary>
    public required long RowsWritten { get; init; }

    /// <summary>Rows rejected or discarded because the buffer was full or the writer was stopped.</summary>
    public required long RowsDropped { get; init; }

    /// <summary>Rows currently waiting in memory.</summary>
    public required int BufferedRows { get; init; }

    /// <summary>Maximum number of buffered rows.</summary>
    public required int Capacity { get; init; }

    /// <summary><see cref="BufferedRows"/> / <see cref="Capacity"/> (0…1).</summary>
    public double FillRatio => Capacity == 0 ? 0 : (double)BufferedRows / Capacity;

    /// <summary>Batches handed to the sinks.</summary>
    public required long BatchesWritten { get; init; }

    /// <summary>UTC time of the last batch write, if any.</summary>
    public required DateTime? LastWriteUtc { get; init; }

    /// <summary>Per-sink counters.</summary>
    public required IReadOnlyList<SinkStatus> Sinks { get; init; }

    /// <inheritdoc />
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append(CultureInfo.InvariantCulture,
            $"buffer {BufferedRows}/{Capacity} ({FillRatio:P1}), enqueued {RowsEnqueued}, written {RowsWritten}, dropped {RowsDropped}, batches {BatchesWritten}");
        foreach (var sink in Sinks)
        {
            sb.Append(CultureInfo.InvariantCulture, $"; {sink.Name}: {sink.RowsWritten} rows");
            if (sink.FailedBatches > 0)
                sb.Append(CultureInfo.InvariantCulture, $", {sink.FailedBatches} failed batches ({sink.LastError})");
        }

        return sb.ToString();
    }
}

/// <summary>Raised when a sink loses a batch after all retries.</summary>
public sealed class SinkErrorEventArgs : EventArgs
{
    /// <summary>Creates the event data.</summary>
    public SinkErrorEventArgs(string sinkName, Exception exception, int rowsLost)
    {
        SinkName = sinkName;
        Exception = exception;
        RowsLost = rowsLost;
    }

    /// <summary>Sink that failed.</summary>
    public string SinkName { get; }

    /// <summary>The failure.</summary>
    public Exception Exception { get; }

    /// <summary>Rows of the lost batch.</summary>
    public int RowsLost { get; }
}
