using System.Threading;
using System.Threading.Tasks;

namespace Tgs.MessageQueue
{
	public interface IMessageExceptionProcessor
	{
		ValueTask<byte[]> HandleExceptionAsync(byte[] data, CancellationToken cancellationToken = default);
	}
}
