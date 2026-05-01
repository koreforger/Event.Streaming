using Event.Streaming.Processing.Envelopes;
using System.Text.Json;
using Xunit;

namespace Event.Streaming.Processing.Tests;

public sealed class OperationalEnvelopeTests
{
    [Fact]
    public void Default_CorrelationId_is_non_empty()
    {
        var envelope = new OperationalEnvelope();
        Assert.False(string.IsNullOrWhiteSpace(envelope.CorrelationId));
    }

    [Fact]
    public void RoutingTags_starts_empty()
    {
        var envelope = new OperationalEnvelope();
        Assert.Empty(envelope.RoutingTags);
    }

    [Fact]
    public void RoutingTags_can_be_mutated()
    {
        var envelope = new OperationalEnvelope();
        envelope.Tags["region"] = "eu-west";
        Assert.Equal("eu-west", envelope.RoutingTags["region"]);
    }

    [Fact]
    public void ProcessingWarnings_starts_empty()
    {
        var envelope = new OperationalEnvelope();
        Assert.Empty(envelope.ProcessingWarnings);
    }

    [Fact]
    public void Payload_roundtrips_simple_json()
    {
        var envelope = new OperationalEnvelope
        {
            Payload = JsonDocument.Parse("{\"amount\":99.5}").RootElement
        };
        Assert.Equal(99.5, envelope.Payload.GetProperty("amount").GetDouble());
    }
}
