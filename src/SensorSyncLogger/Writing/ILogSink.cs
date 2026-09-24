namespace SensorSyncLogger.Writing;

/// <summary>A log destination (file, database…). Called only from the <see cref="BufferedWriter"/> flush thread.</summary>
/// <remarks>
/// Calls are never concurrent for one sink: <see cref="OpenAsync"/> once, then any number of
/// <see cref="WriteBatchAsync"/> / <see cref="FlushAsync"/>, then <see cref="IAsyncDisposable.DisposeAsync"/>.
/// </remarks>
public interface ILogSink : IAsyncDisposable
{
    /// <summary>Human readable name used in status reports and errors.</summary>
    string Name { get; }

    /// <summary>Prepares the destination for rows of <paramref name="schema"/> (create file / tables…).</summary>
    ValueTask OpenAsync(LogSchema schema, CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes a batch of rows. The list is reused after the call returns, so implementations must not keep it.
    /// Implementations should persist the batch as a unit (one transaction, one buffered write).
    /// </summary>
    ValueTask WriteBatchAsync(IReadOnlyList<LogRow> rows, CancellationToken cancellationToken = default);

    /// <summary>Makes everything written so far durable (flush file buffers, commit).</summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
}
