using Event.Streaming.Processing.Pipeline;
using Xunit;

namespace Event.Streaming.Processing.Tests;

public sealed class DiagnosticStageTests
{
    [Theory]
    [InlineData("FullPipeline", DiagnosticStage.FullPipeline)]
    [InlineData("KafkaOnly", DiagnosticStage.KafkaOnly)]
    [InlineData("DecodeOnly", DiagnosticStage.DecodeOnly)]
    [InlineData("ClassifyOnly", DiagnosticStage.ClassifyOnly)]
    [InlineData("ParseOnly", DiagnosticStage.ParseOnly)]
    [InlineData("RulesOnly", DiagnosticStage.RulesOnly)]
    [InlineData("OutputOnly", DiagnosticStage.OutputOnly)]
    [InlineData("SqlOnly", DiagnosticStage.SqlOnly)]
    [InlineData("NullOutput", DiagnosticStage.NullOutput)]
    public void DiagnosticStage_parses_from_string(string name, DiagnosticStage expected)
    {
        var parsed = Enum.Parse<DiagnosticStage>(name);
        Assert.Equal(expected, parsed);
    }
}
