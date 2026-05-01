using Event.Streaming.Out.Routing;
using Xunit;

namespace Event.Streaming.Out.Tests;

public sealed class WriteResultTests
{
    [Fact]
    public void Ok_factory_sets_success_true()
    {
        var result = WriteResult.Ok("output1", "fraud.decisions", 42L, TimeSpan.FromMilliseconds(5));

        Assert.True(result.Success);
        Assert.Equal("output1", result.RouteName);
        Assert.Equal("fraud.decisions", result.TargetTopic);
        Assert.Equal(42L, result.ProducedOffset);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void Fail_factory_sets_success_false()
    {
        var result = WriteResult.Fail("output1", "broker unavailable", TimeSpan.FromMilliseconds(100));

        Assert.False(result.Success);
        Assert.Equal("output1", result.RouteName);
        Assert.Equal("broker unavailable", result.ErrorMessage);
        Assert.Null(result.ProducedOffset);
    }
}
