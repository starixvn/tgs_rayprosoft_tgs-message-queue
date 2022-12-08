using System.Diagnostics;
using Tgs.MessageQueue;

namespace Tgs.MessageQueue.Direct;

internal class DirectDiagnostics
{
	public static ActivitySource ActivitySource = new("Tgs.MessageQueue.Direct");
}
