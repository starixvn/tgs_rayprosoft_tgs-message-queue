using System.Threading;
using System.Threading.Tasks;

namespace Tgs.MessageQueue;

public interface IMessageProcessor
{
	ValueTask<byte[]> HandleAsync(byte[] data, CancellationToken cancellationToken = default);
}
