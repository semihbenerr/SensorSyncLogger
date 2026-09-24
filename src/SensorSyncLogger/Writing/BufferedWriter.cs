using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace SensorSyncLogger.Writing;

/// <summary>
/// Decouples producers (sensor threads) from disk I/O: rows are queued in memory with a lock-free
/// <see cref="ConcurrentQueue{T}"/> and written in batches by one background task, either when
/// <see cref="BufferedWriterOptions.BatchSize"/> rows are waiting or every <see cref="BufferedWriterOptions.FlushInterval"/>.
/// </summary>
/// <remarks>
/// <para><see cref="TryEnqueue"/> never blocks and never performs I/O, so it is safe to call from acquisition threads.</para>
/// <para>Stopping (or disposing) is graceful: new rows are refused, rows already accepted — including those being
/// enqueued at that very moment — are written, sinks are flushed and then disposed. The writer owns its sinks.</para>
/// <para>Each batch is written to all sinks (in parallel when there are several). A failing sink is retried
/// <see cref="BufferedWriterOptions.SinkRetryCount"/> times; if it still fails, the batch is counted as lost for that
/// sink only and <see cref="SinkError"/> is raised — the other sinks keep working.</para>
/// </remarks>
public sealed class BufferedWriter : IAsyncDisposable
{
    private const int StateCreated = 0;
    private const int StateRunning = 1;
    private const int StateStopping = 2;
    private const int StateStopped = 3;

    private readonly ConcurrentQueue<LogRow> _queue = new();
    private readonly ConcurrentQueue<TaskCompletionSource> _flushRequests = new();
    private readonly SemaphoreSlim _signal = new(0, 1);
    private readonly CancellationTokenSource _abort = new();
    private readonly SinkState[] _sinks;
    private readonly BufferedWriterOptions _options;
    private readonly ILogger _logger;
    private readonly object _lifecycleGate = new();

    private int _state = StateCreated;
    private int _count;
    private int _enqueuesInFlight;
    private long _enqueued;
    private long _written;
    private long _dropped;
    private long _batches;
    private long _lastWriteTicks;
    private Task? _loop;
    private Task? _stopTask;

    /// <summary>Creates a writer. Call <see cref="StartAsync"/> before enqueuing rows.</summary>
    /// <param name="schema">Shape of the rows; passed to every sink.</param>
    /// <param name="sinks">Destinations (at least one). They are owned and disposed by the writer.</param>
    /// <param name="options">Tuning; defaults when <c>null</c>.</param>
    /// <param name="logger">Optional logger.</param>
    public BufferedWriter(LogSchema schema, IEnumerable<ILogSink> sinks, BufferedWriterOptions? options = null, ILogger? logger = null)
    {
        Schema = schema ?? throw new ArgumentNullException(nameof(schema));
        ArgumentNullException.ThrowIfNull(sinks);
        _sinks = sinks.Select(s => new SinkState(s ?? throw new ArgumentException("Sinks must not be null.", nameof(sinks)))).ToArray();
        if (_sinks.Length == 0)
            throw new ArgumentException("At least one sink is required.", nameof(sinks));

        _options = (options ?? new BufferedWriterOptions()).Clone();
        _options.Validate();
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>Shape of the rows.</summary>
    public LogSchema Schema { get; }

    /// <summary><c>true</c> while rows are accepted.</summary>
    public bool IsRunning => Volatile.Read(ref _state) == StateRunning;

    /// <summary>Raised (on the writer task) when a sink loses a batch after all retries.</summary>
    public event EventHandler<SinkErrorEventArgs>? SinkError;

    /// <summary>Opens all sinks and starts the background flush task.</summary>
    /// <exception cref="InvalidOperationException">The writer was already started or stopped.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_lifecycleGate)
        {
            if (_state != StateCreated)
                throw new InvalidOperationException("The writer can only be started once.");
            _state = StateStopping; // blocks a concurrent StartAsync while sinks are opened
        }

        try
        {
            foreach (var sink in _sinks)
                await sink.Sink.OpenAsync(Schema, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await DisposeSinksAsync().ConfigureAwait(false);
            Volatile.Write(ref _state, StateStopped);
            throw;
        }

        Volatile.Write(ref _state, StateRunning);
        _loop = Task.Run(RunAsync, CancellationToken.None);
        _logger.LogDebug("Buffered writer started with {SinkCount} sink(s).", _sinks.Length);
    }

    /// <summary>Queues a row without blocking.</summary>
    /// <returns><c>false</c> when the row was rejected (writer not running, or buffer full with <see cref="OverflowPolicy.DropNewest"/>).</returns>
    public bool TryEnqueue(in LogRow row)
    {
        Interlocked.Increment(ref _enqueuesInFlight);
        try
        {
            if (Volatile.Read(ref _state) != StateRunning)
            {
                Interlocked.Increment(ref _dropped);
                return false;
            }

            int count = Interlocked.Increment(ref _count);
            if (count > _options.Capacity)
            {
                if (_options.OverflowPolicy == OverflowPolicy.DropNewest)
                {
                    Interlocked.Decrement(ref _count);
                    Interlocked.Increment(ref _dropped);
                    return false;
                }

                if (_queue.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _count);
                    Interlocked.Increment(ref _dropped);
                }
            }

            _queue.Enqueue(row);
            Interlocked.Increment(ref _enqueued);
            if (count >= _options.BatchSize)
                Signal();
            return true;
        }
        finally
        {
            Interlocked.Decrement(ref _enqueuesInFlight);
        }
    }

    /// <summary>Writes every row accepted before this call and flushes the sinks.</summary>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _state) != StateRunning)
            return Task.CompletedTask;

        var request = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _flushRequests.Enqueue(request);
        Signal();
        return request.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Stops accepting rows, writes everything still buffered, flushes and disposes the sinks.
    /// If <paramref name="cancellationToken"/> is cancelled, writing is aborted and remaining rows are counted as dropped.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_lifecycleGate)
        {
            _stopTask ??= StopCoreAsync(cancellationToken);
            return _stopTask;
        }
    }

    /// <summary>Snapshot of the counters.</summary>
    public BufferStatus GetStatus()
    {
        long lastWrite = Interlocked.Read(ref _lastWriteTicks);
        return new BufferStatus
        {
            IsRunning = IsRunning,
            RowsEnqueued = Interlocked.Read(ref _enqueued),
            RowsWritten = Interlocked.Read(ref _written),
            RowsDropped = Interlocked.Read(ref _dropped),
            BufferedRows = Math.Max(0, Volatile.Read(ref _count)),
            Capacity = _options.Capacity,
            BatchesWritten = Interlocked.Read(ref _batches),
            LastWriteUtc = lastWrite == 0 ? null : new DateTime(lastWrite, DateTimeKind.Utc),
            Sinks = _sinks.Select(s => s.ToStatus()).ToArray(),
        };
    }

    /// <summary>Same as <see cref="StopAsync"/>.</summary>
    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        int previous;
        lock (_lifecycleGate)
        {
            previous = _state;
            if (previous == StateRunning)
                _state = StateStopping;
        }

        if (previous == StateRunning && _loop is not null)
        {
            Signal();
            try
            {
                await _loop.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _abort.Cancel();
                await _loop.ConfigureAwait(false);
                _logger.LogWarning("Buffered writer stop was cancelled; {Rows} buffered row(s) were discarded.", Volatile.Read(ref _count));
                DiscardQueue();
            }
        }

        await DisposeSinksAsync().ConfigureAwait(false);
        Volatile.Write(ref _state, StateStopped);
        _logger.LogDebug("Buffered writer stopped: {Status}", GetStatus());
    }

    private async Task RunAsync()
    {
        var batch = new List<LogRow>(_options.BatchSize);
        var requests = new List<TaskCompletionSource>();
        try
        {
            while (true)
            {
                bool stopping = Volatile.Read(ref _state) >= StateStopping;
                if (!stopping && _flushRequests.IsEmpty && Volatile.Read(ref _count) < _options.BatchSize)
                {
                    await _signal.WaitAsync(_options.FlushInterval, _abort.Token).ConfigureAwait(false);
                    stopping = Volatile.Read(ref _state) >= StateStopping;
                }

                requests.Clear();
                while (_flushRequests.TryDequeue(out var request))
                    requests.Add(request);

                if (stopping)
                    await WaitForPendingEnqueuesAsync().ConfigureAwait(false);

                await DrainAsync(batch).ConfigureAwait(false);

                if (requests.Count > 0 || stopping)
                    await FlushSinksAsync().ConfigureAwait(false);

                foreach (var request in requests)
                    request.TrySetResult();

                if (stopping)
                    break;
            }
        }
        catch (OperationCanceledException) when (_abort.IsCancellationRequested)
        {
            // Stop was cancelled by the caller; remaining rows are discarded by StopCoreAsync.
        }
        finally
        {
            foreach (var request in requests)
                request.TrySetResult();
            while (_flushRequests.TryDequeue(out var request))
                request.TrySetResult();
        }
    }

    private async Task WaitForPendingEnqueuesAsync()
    {
        // TryEnqueue calls that passed the state check before Stop must finish, or their rows would be missed.
        var spin = new SpinWait();
        while (Volatile.Read(ref _enqueuesInFlight) > 0)
        {
            if (spin.NextSpinWillYield)
                await Task.Yield();
            spin.SpinOnce();
        }
    }

    private async Task DrainAsync(List<LogRow> batch)
    {
        while (_queue.TryDequeue(out var row))
        {
            Interlocked.Decrement(ref _count);
            batch.Add(row);
            if (batch.Count >= _options.BatchSize)
            {
                await WriteBatchAsync(batch).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await WriteBatchAsync(batch).ConfigureAwait(false);
            batch.Clear();
        }
    }

    private async Task WriteBatchAsync(List<LogRow> batch)
    {
        bool allSucceeded;
        if (_sinks.Length == 1)
        {
            allSucceeded = await WriteToSinkAsync(_sinks[0], batch).ConfigureAwait(false);
        }
        else
        {
            // Sinks are independent (a CSV file and a database); write them in parallel.
            var results = await Task.WhenAll(_sinks.Select(s => Task.Run(() => WriteToSinkAsync(s, batch)))).ConfigureAwait(false);
            allSucceeded = results.All(ok => ok);
        }

        Interlocked.Increment(ref _batches);
        Interlocked.Exchange(ref _lastWriteTicks, DateTime.UtcNow.Ticks);
        if (allSucceeded)
            Interlocked.Add(ref _written, batch.Count);
    }

    private async Task<bool> WriteToSinkAsync(SinkState sink, List<LogRow> batch)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await sink.Sink.WriteBatchAsync(batch, _abort.Token).ConfigureAwait(false);
                sink.AddWritten(batch.Count);
                return true;
            }
            catch (OperationCanceledException) when (_abort.IsCancellationRequested)
            {
                sink.AddFailure(batch.Count, "Stop was cancelled.");
                return false;
            }
            catch (Exception ex) when (attempt < _options.SinkRetryCount)
            {
                _logger.LogWarning(ex, "Sink {Sink} failed to write {Rows} row(s) (attempt {Attempt}); retrying.",
                    sink.Sink.Name, batch.Count, attempt + 1);
                await Task.Delay(_options.SinkRetryDelay).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                sink.AddFailure(batch.Count, ex.Message);
                _logger.LogError(ex, "Sink {Sink} lost a batch of {Rows} row(s).", sink.Sink.Name, batch.Count);
                RaiseSinkError(sink.Sink.Name, ex, batch.Count);
                return false;
            }
        }
    }

    private async Task FlushSinksAsync()
    {
        foreach (var sink in _sinks)
        {
            try
            {
                await sink.Sink.FlushAsync(_abort.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_abort.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                sink.AddFailure(0, ex.Message);
                _logger.LogError(ex, "Sink {Sink} failed to flush.", sink.Sink.Name);
                RaiseSinkError(sink.Sink.Name, ex, 0);
            }
        }
    }

    private async Task DisposeSinksAsync()
    {
        foreach (var sink in _sinks)
        {
            try
            {
                await sink.Sink.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sink {Sink} failed to close.", sink.Sink.Name);
                RaiseSinkError(sink.Sink.Name, ex, 0);
            }
        }
    }

    private void DiscardQueue()
    {
        while (_queue.TryDequeue(out _))
        {
            Interlocked.Decrement(ref _count);
            Interlocked.Increment(ref _dropped);
        }
    }

    private void Signal()
    {
        if (_signal.CurrentCount != 0)
            return;
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
            // Another thread signalled first; one pending signal is enough.
        }
    }

    private void RaiseSinkError(string sinkName, Exception exception, int rowsLost)
    {
        try
        {
            SinkError?.Invoke(this, new SinkErrorEventArgs(sinkName, exception, rowsLost));
        }
        catch (Exception handlerError)
        {
            _logger.LogError(handlerError, "A SinkError handler threw.");
        }
    }

    private sealed class SinkState
    {
        private long _rowsWritten;
        private long _failedBatches;
        private long _rowsLost;
        private volatile string? _lastError;

        public SinkState(ILogSink sink) => Sink = sink;

        public ILogSink Sink { get; }

        public void AddWritten(int rows) => Interlocked.Add(ref _rowsWritten, rows);

        public void AddFailure(int rows, string message)
        {
            if (rows > 0)
            {
                Interlocked.Increment(ref _failedBatches);
                Interlocked.Add(ref _rowsLost, rows);
            }

            _lastError = message;
        }

        public SinkStatus ToStatus() => new(Sink.Name, Interlocked.Read(ref _rowsWritten),
            Interlocked.Read(ref _failedBatches), Interlocked.Read(ref _rowsLost), _lastError);
    }
}
