using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SensorSyncLogger.Sources;
using SensorSyncLogger.Timing;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Sync;

/// <summary>
/// Collects samples from several <see cref="ISensorSource"/>s running at different rates and logs them through a
/// <see cref="BufferedWriter"/> to one or more <see cref="ILogSink"/>s — either as raw rows (one per sample) or as
/// time-aligned rows (one per <see cref="TimeSyncOptions.AlignInterval"/> with a column per channel).
/// </summary>
/// <remarks>
/// <para>The sample callback only updates counters and enqueues (lock-free) — no I/O ever happens on a sensor thread.</para>
/// <para>An engine runs one session: <see cref="StartAsync"/> → <see cref="StopAsync"/>. It owns the sinks added to it
/// and disposes them when stopped. Stopping is graceful: sources are stopped first, the last aligned rows up to the
/// newest sample are produced, and every buffered row is written before the sinks are closed.</para>
/// </remarks>
public sealed class TimeSyncEngine : IAsyncDisposable
{
    private readonly TimeSyncOptions _options;
    private readonly SyncClock _clock;
    private readonly ILogger _logger;
    private readonly List<ISensorSource> _sources = new();
    private readonly List<ILogSink> _sinks = new();
    private readonly object _gate = new();

    private EngineState _state = EngineState.Created;
    private ChannelCounter[] _counters = Array.Empty<ChannelCounter>();
    private Action<SensorSample>[] _handlers = Array.Empty<Action<SensorSample>>();
    private BufferedWriter? _writer;
    private SampleAligner? _aligner;
    private CancellationTokenSource? _alignerStop;
    private Task? _alignerLoop;
    private Task? _stopTask;
    private DateTime? _startedAtUtc;
    private long _samples;
    private long _rows;

    /// <summary>Creates an engine.</summary>
    /// <param name="options">Configuration; defaults (raw mode) when <c>null</c>.</param>
    /// <param name="logger">Optional logger.</param>
    public TimeSyncEngine(TimeSyncOptions? options = null, ILogger? logger = null)
    {
        _options = options ?? new TimeSyncOptions();
        _options.Validate();
        _clock = _options.Clock ?? SyncClock.Default;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>Current state.</summary>
    public EngineState State
    {
        get
        {
            lock (_gate)
                return _state;
        }
    }

    /// <summary>Logging mode.</summary>
    public LogMode Mode => _options.Mode;

    /// <summary>Registered sources in column order.</summary>
    public IReadOnlyList<ISensorSource> Sources
    {
        get
        {
            lock (_gate)
                return _sources.ToArray();
        }
    }

    /// <summary>Raised when a sink loses a batch after all retries.</summary>
    public event EventHandler<SinkErrorEventArgs>? SinkError;

    /// <summary>Registers a source. Its channel name must be unique (case-insensitive).</summary>
    /// <exception cref="InvalidOperationException">The engine was already started.</exception>
    public TimeSyncEngine AddSource(ISensorSource source)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(source.ChannelName, nameof(source));
        lock (_gate)
        {
            EnsureCreated();
            if (_sources.Any(s => string.Equals(s.ChannelName, source.ChannelName, StringComparison.OrdinalIgnoreCase)))
                throw new ArgumentException($"A source with channel name '{source.ChannelName}' is already registered.", nameof(source));
            _sources.Add(source);
        }

        return this;
    }

    /// <summary>Adds a log destination. The engine takes ownership and disposes it when stopped.</summary>
    /// <exception cref="InvalidOperationException">The engine was already started.</exception>
    public TimeSyncEngine AddSink(ILogSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        lock (_gate)
        {
            EnsureCreated();
            _sinks.Add(sink);
        }

        return this;
    }

    /// <summary>Opens the sinks, subscribes to the sources and (unless disabled) starts them.</summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ISensorSource[] sources;
        ILogSink[] sinks;
        lock (_gate)
        {
            EnsureCreated();
            if (_sources.Count == 0)
                throw new InvalidOperationException("Add at least one source before starting.");
            if (_sinks.Count == 0)
                throw new InvalidOperationException("Add at least one sink before starting.");
            _state = EngineState.Starting;
            sources = _sources.ToArray();
            sinks = _sinks.ToArray();
        }

        try
        {
            var startedAt = _clock.UtcNow;
            var channels = sources.Select(s => new ChannelInfo(s.ChannelName, s.SampleRateHz, s.Unit)).ToArray();
            var schema = new LogSchema(_options.Mode, channels, startedAt,
                _options.Mode == LogMode.Aligned ? _options.AlignInterval : null);

            _counters = channels.Select(_ => new ChannelCounter()).ToArray();
            _writer = new BufferedWriter(schema, sinks, _options.Writer, _logger);
            _writer.SinkError += (_, e) => SinkError?.Invoke(this, e);
            await _writer.StartAsync(cancellationToken).ConfigureAwait(false);

            if (_options.Mode == LogMode.Aligned)
            {
                _aligner = new SampleAligner(channels.Length, startedAt, _options.AlignInterval,
                    ResolveAlignmentDelay(sources), _options.Interpolation, _options.MaxSampleAge);
                _alignerStop = new CancellationTokenSource();
                _alignerLoop = Task.Run(() => AlignLoopAsync(_alignerStop.Token), CancellationToken.None);
            }

            _handlers = new Action<SensorSample>[sources.Length];
            for (int i = 0; i < sources.Length; i++)
            {
                int channelIndex = i;
                _handlers[i] = sample => OnSample(channelIndex, sample);
                sources[i].OnSample += _handlers[i];
            }

            _startedAtUtc = startedAt;
            lock (_gate)
                _state = EngineState.Running;

            if (_options.ManageSourceLifetime)
            {
                foreach (var source in sources)
                    await source.StartAsync(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("Sync engine started: {Mode} mode, {Channels} channel(s), {Sinks} sink(s).",
                _options.Mode, sources.Length, sinks.Length);
        }
        catch
        {
            Task cleanup;
            lock (_gate)
                cleanup = _stopTask ??= StopCoreAsync(sources, CancellationToken.None);
            await cleanup.ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Stops the sources (if managed), writes all remaining data and closes the sinks.
    /// Safe to call more than once; later calls return the same task.
    /// </summary>
    /// <param name="cancellationToken">Cancelling aborts writing; rows not yet written are discarded.</param>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_stopTask is not null)
                return _stopTask;

            if (_state == EngineState.Created)
            {
                _state = EngineState.Stopped;
                _stopTask = DisposeUnopenedSinksAsync(_sinks.ToArray());
                return _stopTask;
            }

            _stopTask = StopCoreAsync(_sources.ToArray(), cancellationToken);
            return _stopTask;
        }
    }

    /// <summary>Writes everything buffered so far and flushes the sinks (aligned rows are produced after their delay).</summary>
    public Task FlushAsync(CancellationToken cancellationToken = default) =>
        _writer?.FlushAsync(cancellationToken) ?? Task.CompletedTask;

    /// <summary>Live counters: samples, rows, buffer fill ratio, per-sink and per-channel figures.</summary>
    public SyncStatus GetStatus()
    {
        EngineState state;
        ISensorSource[] sources;
        lock (_gate)
        {
            state = _state;
            sources = _sources.ToArray();
        }

        var counters = _counters;
        var channels = new ChannelStatus[sources.Length];
        for (int i = 0; i < sources.Length; i++)
        {
            var counter = i < counters.Length ? counters[i] : new ChannelCounter();
            channels[i] = counter.ToStatus(sources[i]);
        }

        var started = _startedAtUtc;
        return new SyncStatus
        {
            State = state,
            Mode = _options.Mode,
            StartedAtUtc = started,
            Uptime = started is { } s ? _clock.UtcNow - s : TimeSpan.Zero,
            SamplesReceived = Interlocked.Read(ref _samples),
            RowsProduced = Interlocked.Read(ref _rows),
            Buffer = _writer?.GetStatus(),
            Channels = channels,
        };
    }

    /// <summary>Same as <see cref="StopAsync"/>.</summary>
    public async ValueTask DisposeAsync() => await StopAsync().ConfigureAwait(false);

    private void OnSample(int channelIndex, SensorSample sample)
    {
        var timestamp = sample.Timestamp.Kind switch
        {
            DateTimeKind.Utc => sample.Timestamp,
            DateTimeKind.Local => sample.Timestamp.ToUniversalTime(),
            _ => DateTime.SpecifyKind(sample.Timestamp, DateTimeKind.Utc),
        };

        _counters[channelIndex].Record(timestamp.Ticks, sample.Value);
        Interlocked.Increment(ref _samples);

        if (_aligner is { } aligner)
        {
            aligner.Add(channelIndex, timestamp, sample.Value);
        }
        else if (_writer!.TryEnqueue(LogRow.Raw(timestamp, channelIndex, sample.Value)))
        {
            Interlocked.Increment(ref _rows);
        }
    }

    private async Task AlignLoopAsync(CancellationToken cancellationToken)
    {
        var rows = new List<LogRow>();
        var tick = TimeSpan.FromMilliseconds(Math.Clamp(_options.AlignInterval.TotalMilliseconds / 2, 10, 250));
        using var timer = new PeriodicTimer(tick, _clock.TimeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                _aligner!.EmitDue(_clock.UtcNow, rows);
                EnqueueRows(rows);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Stopping.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "The alignment loop failed; aligned rows are no longer produced.");
        }
    }

    private void EnqueueRows(List<LogRow> rows)
    {
        foreach (var row in rows)
        {
            if (_writer!.TryEnqueue(row))
                Interlocked.Increment(ref _rows);
        }

        rows.Clear();
    }

    private TimeSpan ResolveAlignmentDelay(IReadOnlyList<ISensorSource> sources)
    {
        if (_options.AlignmentDelay is { } explicitDelay)
            return explicitDelay;
        if (_options.Interpolation == InterpolationMode.LastKnownValue)
            return _options.AlignInterval;

        // Linear interpolation needs the sample after each grid point: wait up to the slowest channel's period.
        double slowestPeriodSeconds = sources.Max(s => 1.0 / s.SampleRateHz);
        var delay = _options.AlignInterval + TimeSpan.FromSeconds(slowestPeriodSeconds);
        return delay > TimeSpan.FromSeconds(10) ? TimeSpan.FromSeconds(10) : delay;
    }

    private async Task StopCoreAsync(ISensorSource[] sources, CancellationToken cancellationToken)
    {
        lock (_gate)
            _state = EngineState.Stopping;

        if (_options.ManageSourceLifetime)
        {
            foreach (var source in sources)
            {
                try
                {
                    await source.StopAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Source {Channel} failed to stop.", source.ChannelName);
                }
            }
        }

        for (int i = 0; i < _handlers.Length && i < sources.Length; i++)
        {
            if (_handlers[i] is { } handler)
                sources[i].OnSample -= handler;
        }

        if (_alignerLoop is not null)
        {
            _alignerStop!.Cancel();
            await _alignerLoop.ConfigureAwait(false);
            _alignerStop.Dispose();

            // Produce the last rows up to the newest sample so no data at the end of the session is lost.
            if (_aligner!.NewestSampleUtc() is { } newest)
            {
                var rows = new List<LogRow>();
                _aligner.EmitUntil(newest, rows);
                EnqueueRows(rows);
            }
        }

        if (_writer is not null)
            await _writer.StopAsync(cancellationToken).ConfigureAwait(false);
        else
            await DisposeUnopenedSinksAsync(_sinks.ToArray()).ConfigureAwait(false);

        lock (_gate)
            _state = EngineState.Stopped;

        var status = GetStatus();
        _logger.LogInformation("Sync engine stopped: {Samples} samples, {Written} rows written, {Dropped} dropped.",
            status.SamplesReceived, status.Buffer?.RowsWritten ?? 0, status.Buffer?.RowsDropped ?? 0);
    }

    private async Task DisposeUnopenedSinksAsync(ILogSink[] sinks)
    {
        foreach (var sink in sinks)
        {
            try
            {
                await sink.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sink {Sink} failed to close.", sink.Name);
            }
        }
    }

    private void EnsureCreated()
    {
        if (_state != EngineState.Created)
            throw new InvalidOperationException($"The engine is {_state}; sources and sinks can only be changed before it starts, and it cannot be restarted.");
    }

    /// <summary>Lock-free per-channel counters (written by one sensor thread, read by status queries).</summary>
    private sealed class ChannelCounter
    {
        private long _count;
        private long _firstTicks;
        private long _lastTicks;
        private double _lastValue = double.NaN;

        public void Record(long ticks, double value)
        {
            if (Interlocked.Increment(ref _count) == 1)
                Interlocked.Exchange(ref _firstTicks, ticks);
            Interlocked.Exchange(ref _lastTicks, ticks);
            Interlocked.Exchange(ref _lastValue, value);
        }

        public ChannelStatus ToStatus(ISensorSource source)
        {
            long count = Interlocked.Read(ref _count);
            long first = Interlocked.Read(ref _firstTicks);
            long last = Interlocked.Read(ref _lastTicks);
            double seconds = (last - first) / (double)TimeSpan.TicksPerSecond;
            double rate = count > 1 && seconds > 0 ? (count - 1) / seconds : 0;
            return new ChannelStatus(source.ChannelName, source.Unit, source.SampleRateHz, count, rate,
                Volatile.Read(ref _lastValue), count == 0 ? null : new DateTime(last, DateTimeKind.Utc));
        }
    }
}
