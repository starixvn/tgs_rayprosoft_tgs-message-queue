using System.Diagnostics;

namespace Tgs.MessageQueue.InProcess;

internal static class InProcessDiagnostics
{
	internal static ActivitySource ActivitySource = new("Tgs.MessageQueue.InProcess");
}
