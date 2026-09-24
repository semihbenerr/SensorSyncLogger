using SensorSyncLogger.Sources;
using SensorSyncLogger.Sync;
using SensorSyncLogger.Timing;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Tests;

public class EngineTests
{
    [Fact]
    public async Task Raw_mode_logs_every_sample_with_its_own_timestamp()
    {
        var a = new ManualSource("A", 100, "N");
        var b = new ManualSource("B", 1);
        var sink = new MemorySink();
        var engine = new TimeSyncEngine().AddSource(a).AddSource(b).AddSink(sink);
        await engine.StartAsync();

        var t = DateTime.UtcNow;
        for (int i = 0; i < 100; i++)
            a.Emit(t.AddMilliseconds(i * 10), i);
        b.Emit(t, 42);
        await engine.StopAsync();

        Assert.Equal(1, a.StartCount);
        Assert.Equal(1, a.StopCount);
        Assert.Equal(101, sink.Rows.Count);
        Assert.Equal(42, sink.Rows.Single(r => r.ChannelIndex == 1).Value);
        Assert.Equal(new[] { "A", "B" }, sink.Schema!.Channels.Select(c => c.Name));
        Assert.Equal("N", sink.Schema.Channels[0].Unit);

        var status = engine.GetStatus();
        Assert.Equal(EngineState.Stopped, status.State);
        Assert.Equal(101, status.SamplesReceived);
        Assert.Equal(101, status.Buffer!.RowsWritten);
        Assert.Equal(100, status.Channels[0].SamplesReceived);
        Assert.Equal(100, status.Channels[0].MeasuredRateHz, 1);
        Assert.Equal(42, status.Channels[1].LastValue);
    }

    [Fact]
    public async Task Aligned_mode_writes_one_row_per_interval_up_to_the_newest_sample()
    {
        var fast = new ManualSource("Fast", 100);
        var slow = new ManualSource("Slow", 1);
        var sink = new MemorySink();
        var engine = new TimeSyncEngine(new TimeSyncOptions { Mode = LogMode.Aligned, AlignInterval = TimeSpan.FromMilliseconds(100) })
            .AddSource(fast).AddSource(slow).AddSink(sink);
        await engine.StartAsync();

        // Samples slightly in the future of the session start, so rows are produced at stop.
        var start = engine.GetStatus().StartedAtUtc!.Value;
        var firstGrid = new DateTime((start.Ticks / TimeSpan.TicksPerSecond + 1) * TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        slow.Emit(firstGrid, 5);
        for (int i = 0; i <= 100; i++)
            fast.Emit(firstGrid.AddMilliseconds(i * 10), i);
        await engine.StopAsync();

        var rows = sink.Rows.Where(r => r.Timestamp >= firstGrid).ToArray();
        Assert.Equal(11, rows.Length); // firstGrid … firstGrid + 1 s
        Assert.All(rows, r => Assert.Equal(TimeSpan.Zero, TimeSpan.FromTicks(r.Timestamp.Ticks % TimeSpan.FromMilliseconds(100).Ticks)));
        Assert.Equal(new[] { 0.0, 5.0 }, rows[0].Values);
        Assert.Equal(new[] { 50.0, 5.0 }, rows[5].Values);
        Assert.Equal(new[] { 100.0, 5.0 }, rows[10].Values);
        Assert.Equal(sink.Rows.Count, engine.GetStatus().RowsProduced);
    }

    [Fact]
    public async Task Engine_lifecycle_is_enforced()
    {
        var engine = new TimeSyncEngine();
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.StartAsync()); // no sources

        engine.AddSource(new ManualSource("A"));
        Assert.Throws<ArgumentException>(() => engine.AddSource(new ManualSource("a")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.StartAsync()); // no sinks

        var sink = new MemorySink();
        engine.AddSink(sink);
        await engine.StartAsync();
        Assert.Throws<InvalidOperationException>(() => engine.AddSource(new ManualSource("B")));

        var stop1 = engine.StopAsync();
        var stop2 = engine.StopAsync();
        Assert.Same(stop1, stop2);
        await stop1;
        Assert.True(sink.Disposed);
        await Assert.ThrowsAsync<InvalidOperationException>(() => engine.StartAsync());
    }

    [Fact]
    public async Task Shared_sources_are_not_started_by_unmanaged_engines()
    {
        var source = new ManualSource("A");
        var engine = new TimeSyncEngine(new TimeSyncOptions { ManageSourceLifetime = false })
            .AddSource(source).AddSink(new MemorySink());

        await engine.StartAsync();
        await engine.StopAsync();

        Assert.Equal(0, source.StartCount);
        Assert.Equal(0, source.StopCount);
    }

    [Fact]
    public async Task Periodic_sources_keep_their_rate_on_the_shared_clock()
    {
        var clockBefore = SyncClock.Default.UtcNow;
        using var source = new FuncSensorSource("Fast", 100, _ => 1);
        var sink = new MemorySink();
        var engine = new TimeSyncEngine().AddSource(source).AddSink(sink);

        await engine.StartAsync();
        await Task.Delay(1000);
        await engine.StopAsync();

        var channel = engine.GetStatus().Channels[0];
        Assert.InRange(channel.SamplesReceived, 85, 110);
        Assert.InRange(channel.MeasuredRateHz, 90, 110);
        Assert.All(sink.Rows, r => Assert.True(r.Timestamp >= clockBefore));
        Assert.Equal(sink.Rows.OrderBy(r => r.Timestamp).Select(r => r.Timestamp), sink.Rows.Select(r => r.Timestamp));
    }

    [Fact]
    public async Task A_throwing_read_is_counted_and_sampling_continues()
    {
        int calls = 0;
        using var source = new FuncSensorSource("Flaky", 200, _ => ++calls % 2 == 0 ? throw new TimeoutException() : calls);
        int published = 0;
        source.OnSample += _ => Interlocked.Increment(ref published);

        await source.StartAsync();
        await Task.Delay(300);
        await source.StopAsync();

        Assert.True(source.ReadErrors > 5);
        Assert.True(published > 5);
        Assert.False(source.IsRunning);
    }

    [Fact]
    public void SyncClock_is_monotonic_and_close_to_wall_time()
    {
        var clock = new SyncClock();
        var previous = clock.UtcNow;
        for (int i = 0; i < 100_000; i++)
        {
            var now = clock.UtcNow;
            Assert.True(now >= previous);
            previous = now;
        }

        Assert.InRange((clock.UtcNow - DateTime.UtcNow).Duration(), TimeSpan.Zero, TimeSpan.FromMilliseconds(100));
        Assert.Equal(DateTimeKind.Utc, clock.UtcNow.Kind);
    }
}
