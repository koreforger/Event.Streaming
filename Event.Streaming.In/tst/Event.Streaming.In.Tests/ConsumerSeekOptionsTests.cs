using Event.Streaming.In.Seek;
using Xunit;

namespace Event.Streaming.In.Tests;

public sealed class ConsumerSeekOptionsTests
{
    [Fact]
    public void TryParseStartAsOffset_succeeds_for_long_string()
    {
        var opts = new ConsumerSeekOptions { StartOffsetOrTimestamp = "84000" };
        Assert.True(opts.TryParseStartAsOffset(out var offset));
        Assert.Equal(84000L, offset);
    }

    [Fact]
    public void TryParseStartAsOffset_fails_for_timestamp_string()
    {
        var opts = new ConsumerSeekOptions { StartOffsetOrTimestamp = "2026-04-27T10:00:00Z" };
        Assert.False(opts.TryParseStartAsOffset(out _));
    }

    [Fact]
    public void TryParseStartAsTimestamp_succeeds_for_iso8601_utc()
    {
        var opts = new ConsumerSeekOptions { StartOffsetOrTimestamp = "2026-04-27T10:00:00Z" };
        Assert.True(opts.TryParseStartAsTimestamp(out var ts));
        Assert.Equal(2026, ts.Year);
        Assert.Equal(4, ts.Month);
        Assert.Equal(27, ts.Day);
        Assert.Equal(10, ts.Hour);
        Assert.Equal(TimeSpan.Zero, ts.Offset);
    }

    [Fact]
    public void TryParseStartAsTimestamp_fails_for_plain_offset()
    {
        var opts = new ConsumerSeekOptions { StartOffsetOrTimestamp = "84000" };
        Assert.False(opts.TryParseStartAsTimestamp(out _));
    }

    [Fact]
    public void HasStopBoundary_false_when_stop_is_null()
    {
        var opts = new ConsumerSeekOptions { StopOffsetOrTimestamp = null };
        Assert.False(opts.HasStopBoundary);
    }

    [Fact]
    public void HasStopBoundary_true_when_stop_is_set()
    {
        var opts = new ConsumerSeekOptions { StopOffsetOrTimestamp = "92000" };
        Assert.True(opts.HasStopBoundary);
    }

    [Fact]
    public void TryParseStopAsOffset_succeeds_for_long_string()
    {
        var opts = new ConsumerSeekOptions { StopOffsetOrTimestamp = "92000" };
        Assert.True(opts.TryParseStopAsOffset(out var offset));
        Assert.Equal(92000L, offset);
    }

    [Fact]
    public void TryParseStopAsTimestamp_succeeds_for_iso8601()
    {
        var opts = new ConsumerSeekOptions { StopOffsetOrTimestamp = "2026-04-27T11:00:00Z" };
        Assert.True(opts.TryParseStopAsTimestamp(out var ts));
        Assert.Equal(11, ts.Hour);
    }

    [Fact]
    public void Mode_defaults_to_None()
    {
        var opts = new ConsumerSeekOptions();
        Assert.Equal(SeekMode.None, opts.Mode);
    }

    [Theory]
    [InlineData("None", SeekMode.None)]
    [InlineData("FromOffset", SeekMode.FromOffset)]
    [InlineData("FromTimestamp", SeekMode.FromTimestamp)]
    [InlineData("Range", SeekMode.Range)]
    public void SeekMode_parses_from_string(string name, SeekMode expected)
    {
        var parsed = Enum.Parse<SeekMode>(name);
        Assert.Equal(expected, parsed);
    }
}
