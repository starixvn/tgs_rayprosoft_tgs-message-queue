using System.Threading;
using System.Threading.Tasks;

namespace Tgs.MessageQueue.MongoDB.IntegrationTests;

internal class StubMessageHandler : IMessageHandler
{
	public ValueTask HandleAsync(byte[] data, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
}
