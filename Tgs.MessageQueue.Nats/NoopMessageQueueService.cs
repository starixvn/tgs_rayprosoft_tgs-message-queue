using System;
using System.Threading.Tasks;

namespace Tgs.MessageQueue.Nats
{
	internal class NoopMessageQueueService
		: IMessageSender, IMessageReceiver<NatsSubscriptionSettings>, IMessageReceiver<NatsQueueScriptionSettings>
	{
		public ValueTask PublishAsync(string subject, byte[] data)
		{
			return ValueTask.CompletedTask;
		}

		public ValueTask<byte[]> RequestAsync(string subject, byte[] data)
		{
			return ValueTask.FromResult(Array.Empty<byte>());
		}

		public ValueTask SendAsync(string subject, byte[] data)
		{
			return ValueTask.CompletedTask;
		}

		public ValueTask<IDisposable> SubscribeAsync(NatsQueueScriptionSettings settings)
		{
			return ValueTask.FromResult<IDisposable>(null!);
		}

		public ValueTask<IDisposable> SubscribeAsync(NatsSubscriptionSettings settings)
		{
			return ValueTask.FromResult<IDisposable>(null!);
		}
	}
}
