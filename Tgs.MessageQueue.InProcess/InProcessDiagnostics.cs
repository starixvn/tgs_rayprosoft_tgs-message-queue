using System.Diagnostics;

namespace Tgs.MessageQueue.InProcess;

internal static class InProcessDiagnostics
{
	public static ActivitySource ActivitySource = new("Tgs.MessageQueue.InProcess");
}
