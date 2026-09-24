using SensorSyncLogger.Sync;
using SensorSyncLogger.Writing;

namespace SensorSyncLogger.Tests;

public class SampleAlignerTests
{
    private static readonly DateTime T0 = new(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);

    private static DateTime Ms(int milliseconds) => T0.AddMilliseconds(milliseconds);

    private static SampleAligner Create(int channels, InterpolationMode mode = InterpolationMode.LastKnownValue,
        TimeSpan? maxAge = null, int startMs = 37, int delayMs = 100) =>
        new(channels, Ms(startMs), TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(delayMs), mode, maxAge);

    [Fact]
    public void First_grid_point_is_rounded_up_to_the_interval()
    {
        var aligner = Create(1, startMs: 37);

        Assert.Equal(Ms(100), aligner.NextGridTimeUtc);
    }

    [Fact]
    public void Last_known_value_holds_the_latest_sample_per_channel()
    {
        var aligner = Create(2);
        aligner.Add(0, Ms(50), 1);
        aligner.Add(0, Ms(150), 2);
        aligner.Add(1, Ms(120), 10);

        var rows = new List<LogRow>();
        aligner.EmitUntil(Ms(300), rows);

        Assert.Equal(new[] { Ms(100), Ms(200), Ms(300) }, rows.Select(r => r.Timestamp));
        Assert.Equal(1, rows[0].Values![0]);
        Assert.True(double.IsNaN(rows[0].Values![1])); // channel 1 has no data yet at .100
        Assert.Equal(new[] { 2.0, 10.0 }, rows[1].Values);
        Assert.Equal(new[] { 2.0, 10.0 }, rows[2].Values);
    }

    [Fact]
    public void Leading_rows_without_any_data_are_skipped_but_later_gaps_are_kept()
    {
        var aligner = Create(1, maxAge: TimeSpan.FromMilliseconds(150));
        aligner.Add(0, Ms(250), 5);

        var rows = new List<LogRow>();
        aligner.EmitUntil(Ms(600), rows);

        Assert.Equal(Ms(300), rows[0].Timestamp); // .100 and .200 had no data at all
        Assert.Equal(5, rows[0].Values![0]);
        Assert.True(double.IsNaN(rows[^1].Values![0])); // .600 is 350 ms after the last sample: stale
    }

    [Fact]
    public void Linear_mode_interpolates_between_neighbours()
    {
        var aligner = Create(1, InterpolationMode.Linear);
        aligner.Add(0, Ms(50), 0);
        aligner.Add(0, Ms(150), 10);
        aligner.Add(0, Ms(250), 30);

        var rows = new List<LogRow>();
        aligner.EmitUntil(Ms(200), rows);

        Assert.Equal(5, rows[0].Values![0], 9);   // halfway between 0 and 10
        Assert.Equal(20, rows[1].Values![0], 9);  // halfway between 10 and 30
    }

    [Fact]
    public void Linear_mode_holds_when_the_next_sample_has_not_arrived()
    {
        var aligner = Create(1, InterpolationMode.Linear);
        aligner.Add(0, Ms(50), 7);

        var rows = new List<LogRow>();
        aligner.EmitUntil(Ms(100), rows);

        Assert.Equal(7, rows[0].Values![0]);
    }

    [Fact]
    public void EmitDue_waits_for_the_alignment_delay()
    {
        var aligner = Create(1, delayMs: 100);
        aligner.Add(0, Ms(40), 1);

        var rows = new List<LogRow>();
        aligner.EmitDue(Ms(250), rows);   // grid points up to .150 are due

        Assert.Single(rows);
        Assert.Equal(Ms(100), rows[0].Timestamp);

        aligner.EmitDue(Ms(260), rows);   // nothing new is due
        Assert.Single(rows);
    }

    [Fact]
    public void Out_of_order_samples_are_placed_correctly()
    {
        var aligner = Create(1);
        aligner.Add(0, Ms(90), 2);
        aligner.Add(0, Ms(60), 1);   // arrives late

        var rows = new List<LogRow>();
        aligner.EmitUntil(Ms(100), rows);

        Assert.Equal(2, rows[0].Values![0]);
        Assert.Equal(Ms(90), aligner.NewestSampleUtc());
    }
}
