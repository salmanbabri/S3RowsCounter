using Spectre.Console;
using Spectre.Console.Rendering;

namespace S3RowsCounter;

public class CountColumn : ProgressColumn
{
    public override IRenderable Render(RenderOptions options, ProgressTask task, TimeSpan deltaTime)
    {
        var style = task.Percentage == 100 ? new Style(foreground: Color.Green) : Style.Plain;
        return new Text($"{task.Value}/{task.MaxValue}", style).RightJustified();
    }
}