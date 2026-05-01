using Event.Streaming.Out.Dlq;
using Event.Streaming.Processing.Envelopes;
using Xunit;

namespace Event.Streaming.Out.Tests;

public sealed class DlqTests
{
    [Fact]
    public void NullDlqWriter_is_not_enabled()
    {
        Assert.False(NullDlqWriter.Instance.IsEnabled);
    }

    [Fact]
    public async Task NullDlqWriter_returns_false_and_does_not_throw()
    {
        var result = await NullDlqWriter.Instance.WriteAsync(
            new OperationalEnvelope(),
            new DlqMetadata { FailureReason = "parse error", ProcessorApp = "EventReader", ProcessorInstance = "1" },
            CancellationToken.None);

        Assert.False(result);
    }
}
