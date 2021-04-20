using System.Threading.Tasks;

namespace Tgs.MessageQueue
{
	public interface IMessageSender
	{
		ValueTask PublishAsync(string subject, byte[] data);

		ValueTask SendAsync(string subject, byte[] data);

		ValueTask<byte[]> RequestAsync(string subject, byte[] data);
	}
}
