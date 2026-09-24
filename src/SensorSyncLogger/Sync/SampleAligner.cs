using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Sync;

/// <summary>
/// Turns per-channel sample streams into rows on a common time grid. Pure logic (no timers, no threads of its own):
/// <see cref="Add"/> is thread-safe and may be called from sensor threads; <see cref="EmitDue"/> / <see cref="EmitUntil"/>
/// must be called from one thread at a time.
/// </summary>
internal sealed class SampleAligner
{
    private readonly ChannelHistory[] _channels;
    private readonly long _intervalTicks;
    private readonly long _delayTicks;
    private readonly long _maxAgeTicks;
    private readonly InterpolationMode _mode;
    private long _nextGridTicks;
    private bool _started;

    /// <param name="channelCount">Number of channels (column order = index).</param>
    /// <param name="startUtc">Session start; the first grid point is the first multiple of the interval at or after it.</param>
    /// <param name="interval">Grid interval.</param>
    /// <param name="delay">How long a grid point waits for late samples.</param>
    /// <param name="mode">Value derivation.</param>
    /// <param name="maxSampleAge">Values older than this become NaN; <c>null</c> = hold forever.</param>
    public SampleAligner(int channelCount, DateTime startUtc, TimeSpan interval, TimeSpan delay,
        InterpolationMode mode, TimeSpan? maxSampleAge)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(channelCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);

        _channels = Enumerable.Range(0, channelCount).Select(_ => new ChannelHistory()).ToArray();
        _intervalTicks = interval.Ticks;
        _delayTicks = Math.Max(0, delay.Ticks);
        _maxAgeTicks = maxSampleAge?.Ticks ?? long.MaxValue;
        _mode = mode;

        // Round up to a multiple of the interval so rows land on "nice" times (…00.100, …00.200).
        long start = startUtc.Ticks;
        _nextGridTicks = (start + _intervalTicks - 1) / _intervalTicks * _intervalTicks;
    }

    /// <summary>Time of the next row that will be produced.</summary>
    public DateTime NextGridTimeUtc => new(Volatile.Read(ref _nextGridTicks), DateTimeKind.Utc);

    /// <summary>Records a sample (thread-safe).</summary>
    public void Add(int channelIndex, DateTime timestampUtc, double value) =>
        _channels[channelIndex].Add(timestampUtc.Ticks, value);

    /// <summary>Produces every grid row whose time plus the alignment delay has passed.</summary>
    /// <returns>Number of rows added to <paramref name="output"/>.</returns>
    public int EmitDue(DateTime nowUtc, List<LogRow> output) => EmitThrough(nowUtc.Ticks - _delayTicks, output);

    /// <summary>
    /// Produces every grid row up to <paramref name="untilUtc"/> without waiting for the delay — used at shutdown
    /// with the time of the newest sample, so the log ends where the data ends.
    /// </summary>
    public int EmitUntil(DateTime untilUtc, List<LogRow> output) => EmitThrough(untilUtc.Ticks, output);

    /// <summary>Timestamp of the newest sample of any channel, or <c>null</c> when nothing was received.</summary>
    public DateTime? NewestSampleUtc()
    {
        long newest = long.MinValue;
        foreach (var channel in _channels)
            newest = Math.Max(newest, channel.NewestTicks);
        return newest == long.MinValue ? null : new DateTime(newest, DateTimeKind.Utc);
    }

    private int EmitThrough(long lastGridTicks, List<LogRow> output)
    {
        int produced = 0;
        long grid = _nextGridTicks;
        while (grid <= lastGridTicks)
        {
            var values = new double[_channels.Length];
            bool anyValue = false;
            for (int c = 0; c < _channels.Length; c++)
            {
                values[c] = _channels[c].ValueAt(grid, _mode, _maxAgeTicks);
                anyValue |= !double.IsNaN(values[c]);
            }

            // Leading rows before the first sample of any channel carry no information; later gaps are kept.
            if (anyValue || _started)
            {
                _started = true;
                output.Add(LogRow.Aligned(new DateTime(grid, DateTimeKind.Utc), values));
                produced++;
            }

            grid += _intervalTicks;
        }

        Volatile.Write(ref _nextGridTicks, grid);
        return produced;
    }

    /// <summary>Time-ordered recent samples of one channel.</summary>
    private sealed class ChannelHistory
    {
        private readonly object _gate = new();
        private readonly List<(long Ticks, double Value)> _samples = new();
        private long _newestTicks = long.MinValue;

        public long NewestTicks => Volatile.Read(ref _newestTicks);

        public void Add(long ticks, double value)
        {
            lock (_gate)
            {
                int count = _samples.Count;
                if (count == 0 || ticks >= _samples[count - 1].Ticks)
                {
                    _samples.Add((ticks, value));
                }
                else
                {
                    // Out of order (rare): insert at the right position.
                    int index = _samples.BinarySearch((ticks, value), TicksComparer.Instance);
                    _samples.Insert(index < 0 ? ~index : index + 1, (ticks, value));
                }

                if (ticks > _newestTicks)
                    Volatile.Write(ref _newestTicks, ticks);
            }
        }

        /// <summary>Value at <paramref name="grid"/>; also forgets samples that can no longer be needed.</summary>
        public double ValueAt(long grid, InterpolationMode mode, long maxAgeTicks)
        {
            lock (_gate)
            {
                // Index of the newest sample at or before the grid time (scan from the end: only recent samples follow it).
                int prev = _samples.Count - 1;
                while (prev >= 0 && _samples[prev].Ticks > grid)
                    prev--;

                // Everything older than 'prev' is irrelevant for this and all later grid points.
                if (prev > 0)
                {
                    _samples.RemoveRange(0, prev);
                    prev = 0;
                }

                if (prev < 0)
                    return double.NaN; // no data yet at this time

                var before = _samples[prev];
                if (grid - before.Ticks > maxAgeTicks)
                    return double.NaN; // stale: the sensor stopped reporting

                if (mode == InterpolationMode.Linear && before.Ticks != grid && prev + 1 < _samples.Count)
                {
                    var after = _samples[prev + 1];
                    if (double.IsFinite(before.Value) && double.IsFinite(after.Value))
                    {
                        double fraction = (double)(grid - before.Ticks) / (after.Ticks - before.Ticks);
                        return before.Value + (after.Value - before.Value) * fraction;
                    }
                }

                return before.Value;
            }
        }

        private sealed class TicksComparer : IComparer<(long Ticks, double Value)>
        {
            public static readonly TicksComparer Instance = new();

            public int Compare((long Ticks, double Value) x, (long Ticks, double Value) y) => x.Ticks.CompareTo(y.Ticks);
        }
    }
}
