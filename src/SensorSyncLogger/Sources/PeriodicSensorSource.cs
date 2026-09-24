using System.Diagnostics;
using SensorSyncLogger.Timing;

namespace SensorSyncLogger.Sources;

/// <summary>
/// Samples a value at a fixed rate on a dedicated thread with sub-millisecond pacing
/// (see <see cref="PrecisionWaiter"/>). Derive from it and implement <see cref="ReadValue"/>,
/// or use <see cref="FuncSensorSource"/>.
/// </summary>
/// <remarks>
/// <para>Ticks are scheduled on an absolute grid (start + n × period), so the rate does not drift even if a single
/// read is slow. If the loop falls more than <see cref="MaxLagPeriods"/> periods behind (e.g. the machine was
/// suspended), the missed ticks are skipped instead of being produced in a burst; they are counted in
/// <see cref="MissedTicks"/>.</para>
/// <para>Each sample is stamped with the actual acquisition time from <see cref="SensorSourceBase.Clock"/>.</para>
/// </remarks>
public abstract class PeriodicSensorSource : SensorSourceBase, IDisposable
{
    private readonly object _gate = new();
    private CancellationTokenSource? _stop;
    private Thread? _thread;
    private TaskCompletionSource? _stopped;
    private long _samplesProduced;
    private long _missedTicks;
    private long _readErrors;

    /// <inheritdoc />
    protected PeriodicSensorSource(string channelName, double sampleRateHz, string? unit = null, SyncClock? clock = null)
        : base(channelName, sampleRateHz, unit, clock)
    {
        if (sampleRateHz > 10_000)
            throw new ArgumentOutOfRangeException(nameof(sampleRateHz), sampleRateHz, "Periodic sources support up to 10 kHz; use block acquisition for higher rates.");
    }

    /// <summary>How many periods the loop may lag before missed ticks are skipped. Defaults to 5.</summary>
    public int MaxLagPeriods { get; init; } = 5;

    /// <summary>Number of samples published so far.</summary>
    public long SamplesProduced => Interlocked.Read(ref _samplesProduced);

    /// <summary>Ticks skipped because the loop fell too far behind.</summary>
    public long MissedTicks => Interlocked.Read(ref _missedTicks);

    /// <summary>Exceptions thrown by <see cref="ReadValue"/> (the loop keeps running).</summary>
    public long ReadErrors => Interlocked.Read(ref _readErrors);

    /// <summary><c>true</c> while the sampling thread runs.</summary>
    public bool IsRunning
    {
        get
        {
            lock (_gate)
                return _thread is not null;
        }
    }

    /// <summary>Raised on the sampling thread when <see cref="ReadValue"/> throws.</summary>
    public event Action<PeriodicSensorSource, Exception>? ReadFailed;

    /// <summary>Reads the current value. Called on the sampling thread; must not block longer than one period.</summary>
    /// <param name="timestampUtc">Acquisition time of this sample.</param>
    protected abstract double ReadValue(DateTime timestampUtc);

    /// <inheritdoc />
    public override Task StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_thread is not null)
                return Task.CompletedTask; // already running: starting twice is harmless

            _stop = new CancellationTokenSource();
            _stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var token = _stop.Token;
            var stopped = _stopped;
            _thread = new Thread(() => Run(token, stopped))
            {
                IsBackground = true,
                Name = $"SensorSyncLogger sampler: {ChannelName}",
                Priority = ThreadPriority.AboveNormal,
            };
            _thread.Start();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task stopped;
        lock (_gate)
        {
            if (_thread is null || _stop is null || _stopped is null)
                return;
            _stop.Cancel();
            stopped = _stopped.Task;
        }

        await stopped.WaitAsync(cancellationToken).ConfigureAwait(false);

        lock (_gate)
        {
            _stop?.Dispose();
            _stop = null;
            _thread = null;
            _stopped = null;
        }
    }

    /// <summary>Stops the sampling thread (blocking).</summary>
    public void Dispose()
    {
        StopAsync().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    private void Run(CancellationToken token, TaskCompletionSource stopped)
    {
        try
        {
            using var waiter = new PrecisionWaiter();
            long period = (long)Math.Max(1, Math.Round(Stopwatch.Frequency / SampleRateHz));
            long start = Stopwatch.GetTimestamp();
            long tick = 0;

            while (waiter.WaitUntil(start + tick * period, token))
            {
                SampleOnce();
                tick++;

                // Catch up after a stall by skipping ticks instead of producing a burst of samples.
                long behind = (Stopwatch.GetTimestamp() - (start + tick * period)) / period;
                if (behind > MaxLagPeriods)
                {
                    tick += behind;
                    Interlocked.Add(ref _missedTicks, behind);
                }
            }
        }
        finally
        {
            stopped.TrySetResult();
        }
    }

    private void SampleOnce()
    {
        var timestamp = Clock.UtcNow;
        double value;
        try
        {
            value = ReadValue(timestamp);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _readErrors);
            try
            {
                ReadFailed?.Invoke(this, ex);
            }
            catch (Exception)
            {
                // A faulty error handler must not stop the sampling thread.
            }

            return;
        }

        Publish(timestamp, value);
        Interlocked.Increment(ref _samplesProduced);
    }
}

/// <summary>A <see cref="PeriodicSensorSource"/> whose value comes from a delegate.</summary>
public sealed class FuncSensorSource : PeriodicSensorSource
{
    private readonly Func<DateTime, double> _read;

    /// <summary>Creates the source.</summary>
    /// <param name="channelName">Unique channel name.</param>
    /// <param name="sampleRateHz">Sampling frequency in Hz.</param>
    /// <param name="read">Returns the value for the given UTC acquisition time.</param>
    /// <param name="unit">Optional engineering unit.</param>
    /// <param name="clock">Clock used for timestamps; <see cref="SyncClock.Default"/> when <c>null</c>.</param>
    public FuncSensorSource(string channelName, double sampleRateHz, Func<DateTime, double> read, string? unit = null, SyncClock? clock = null)
        : base(channelName, sampleRateHz, unit, clock)
    {
        _read = read ?? throw new ArgumentNullException(nameof(read));
    }

    /// <inheritdoc />
    protected override double ReadValue(DateTime timestampUtc) => _read(timestampUtc);
}
