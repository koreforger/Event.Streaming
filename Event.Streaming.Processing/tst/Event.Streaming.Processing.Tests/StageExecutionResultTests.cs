using Event.Streaming.Processing.Pipeline;
using Xunit;

namespace Event.Streaming.Processing.Tests;

public sealed class StageExecutionResultTests
{
    [Fact]
    public void Ok_sets_success_true_and_output()
    {
        var elapsed = TimeSpan.FromMilliseconds(42);
        var result = StageExecutionResult<string>.Ok("hello", elapsed);

        Assert.True(result.Success);
        Assert.Equal("hello", result.Output);
        Assert.Equal(elapsed, result.ExecutionTime);
        Assert.Null(result.ErrorCategory);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void Fail_sets_success_false_and_error_details()
    {
        var elapsed = TimeSpan.FromMilliseconds(10);
        var result = StageExecutionResult<string>.Fail("Parse", "bad json", elapsed);

        Assert.False(result.Success);
        Assert.Null(result.Output);
        Assert.Equal("Parse", result.ErrorCategory);
        Assert.Equal("bad json", result.ErrorMessage);
        Assert.Equal(elapsed, result.ExecutionTime);
    }
}
