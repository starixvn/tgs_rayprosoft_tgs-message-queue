using System.Threading;
using System.Threading.Tasks;

namespace Tgs.MessageQueue;

public interface IMessageHandler
{
	ValueTask HandleAsync(byte[] data, CancellationToken cancellationToken = default);
}
