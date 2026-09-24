using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Tests;

public class BufferedWriterTests
{
    private static LogRow Row(int i) => LogRow.Raw(TestSchema.Start.AddMilliseconds(i), 0, i);

    [Fact]
    public async Task Stop_writes_every_accepted_row_in_batches()
    {
        var sink = new MemorySink();
        var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { sink },
            new BufferedWriterOptions { BatchSize = 1000, FlushInterval = TimeSpan.FromHours(1) });
        await writer.StartAsync();

        for (int i = 0; i < 2500; i++)
            Assert.True(writer.TryEnqueue(Row(i)));
        await writer.StopAsync();

        Assert.Equal(Enumerable.Range(0, 2500).Select(i => (double)i), sink.Rows.Select(r => r.Value));
        Assert.All(sink.BatchSizes, size => Assert.InRange(size, 1, 1000));
        Assert.True(sink.Disposed);
        Assert.True(sink.FlushCount >= 1);

        var status = writer.GetStatus();
        Assert.Equal(2500, status.RowsWritten);
        Assert.Equal(0, status.BufferedRows);
        Assert.False(status.IsRunning);
    }

    [Fact]
    public async Task Rows_are_written_after_the_flush_interval_without_filling_a_batch()
    {
        var sink = new MemorySink();
        await using var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { sink },
            new BufferedWriterOptions { BatchSize = 1000, FlushInterval = TimeSpan.FromMilliseconds(50) });
        await writer.StartAsync();

        for (int i = 0; i < 10; i++)
            writer.TryEnqueue(Row(i));

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (sink.Rows.Count < 10 && DateTime.UtcNow < deadline)
            await Task.Delay(10);

        Assert.Equal(10, sink.Rows.Count);
    }

    [Fact]
    public async Task FlushAsync_writes_pending_rows_immediately()
    {
        var sink = new MemorySink();
        await using var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { sink },
            new BufferedWriterOptions { BatchSize = 1000, FlushInterval = TimeSpan.FromHours(1) });
        await writer.StartAsync();

        for (int i = 0; i < 3; i++)
            writer.TryEnqueue(Row(i));
        await writer.FlushAsync().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(3, sink.Rows.Count);
        Assert.Equal(1, sink.FlushCount);
    }

    [Theory]
    [InlineData(OverflowPolicy.DropNewest)]
    [InlineData(OverflowPolicy.DropOldest)]
    public async Task Full_buffer_drops_according_to_the_policy(OverflowPolicy policy)
    {
        var sink = new MemorySink { BlockNextWrite = new SemaphoreSlim(0) };
        var gate = sink.BlockNextWrite;
        var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { sink }, new BufferedWriterOptions
        {
            BatchSize = 10,
            Capacity = 10,
            FlushInterval = TimeSpan.FromHours(1),
            OverflowPolicy = policy,
        });
        await writer.StartAsync();

        // Row 0 is taken by the writer, which then blocks inside the sink: the buffer can fill up.
        writer.TryEnqueue(Row(0));
        _ = writer.FlushAsync();
        await sink.WriteEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        for (int i = 1; i <= 10; i++)
            Assert.True(writer.TryEnqueue(Row(i)));
        bool eleventhAccepted = writer.TryEnqueue(Row(11));

        gate.Release();
        await writer.StopAsync();

        var values = sink.Rows.Select(r => r.Value).ToArray();
        Assert.Equal(1, writer.GetStatus().RowsDropped);
        if (policy == OverflowPolicy.DropNewest)
        {
            Assert.False(eleventhAccepted);
            Assert.Equal(Enumerable.Range(0, 11).Select(i => (double)i), values);
        }
        else
        {
            Assert.True(eleventhAccepted);
            Assert.DoesNotContain(1.0, values);      // oldest buffered row was discarded
            Assert.Contains(11.0, values);
        }
    }

    [Fact]
    public async Task A_failing_sink_does_not_affect_the_others()
    {
        var broken = new MemorySink("broken") { AlwaysFail = true };
        var healthy = new MemorySink("healthy");
        var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { broken, healthy },
            new BufferedWriterOptions { BatchSize = 100, SinkRetryCount = 1, SinkRetryDelay = TimeSpan.Zero });
        var errors = new List<SinkErrorEventArgs>();
        writer.SinkError += (_, e) =>
        {
            lock (errors)
                errors.Add(e);
        };
        await writer.StartAsync();

        for (int i = 0; i < 250; i++)
            writer.TryEnqueue(Row(i));
        await writer.StopAsync();

        Assert.Equal(250, healthy.Rows.Count);
        var status = writer.GetStatus();
        var brokenStatus = status.Sinks.Single(s => s.Name == "broken");
        Assert.Equal(250, brokenStatus.RowsLost);
        Assert.Equal("disk full", brokenStatus.LastError);
        Assert.Equal(250, status.Sinks.Single(s => s.Name == "healthy").RowsWritten);
        Assert.Equal(0, status.RowsWritten); // not every sink succeeded
        Assert.NotEmpty(errors);
        Assert.True(broken.Disposed);
    }

    [Fact]
    public async Task Rows_are_refused_before_start_and_after_stop()
    {
        var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { new MemorySink() });

        Assert.False(writer.TryEnqueue(Row(0)));
        await writer.StartAsync();
        await writer.StopAsync();
        Assert.False(writer.TryEnqueue(Row(1)));
        Assert.Equal(2, writer.GetStatus().RowsDropped);
        await Assert.ThrowsAsync<InvalidOperationException>(() => writer.StartAsync());
    }

    [Fact]
    public async Task Concurrent_producers_lose_nothing()
    {
        var sink = new MemorySink();
        var writer = new BufferedWriter(TestSchema.Raw("A"), new[] { sink },
            new BufferedWriterOptions { BatchSize = 500, FlushInterval = TimeSpan.FromMilliseconds(20) });
        await writer.StartAsync();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(p => Task.Run(() =>
        {
            for (int i = 0; i < 5000; i++)
                writer.TryEnqueue(Row(p * 10_000 + i));
        })));
        await writer.StopAsync();

        Assert.Equal(40_000, sink.Rows.Count);
        Assert.Equal(40_000, sink.Rows.Select(r => r.Value).Distinct().Count());
    }
}
