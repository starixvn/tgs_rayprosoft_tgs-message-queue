using System;
using System.Threading.Tasks;
using NATS.Client;

namespace Tgs.MessageQueue.Nats
{
	internal class NatsMessageQueueService
		: IMessageSender, IMessageReceiver<NatsSubscriptionSettings>, IMessageReceiver<NatsQueueScriptionSettings>
	{
		private readonly IConnection _connection;

		public NatsMessageQueueService(IConnection connection)
		{
			_connection = connection ?? throw new ArgumentNullException(nameof(connection));
		}

		public ValueTask PublishAsync(string subject, byte[] data)
		{
			_connection.Publish(subject, data);

			return ValueTask.CompletedTask;
		}

		public async ValueTask<byte[]> RequestAsync(string subject, byte[] data)
		{
			var reply = await _connection.RequestAsync(subject, data).ConfigureAwait(false);

			return reply.Data;
		}

		public async ValueTask SendAsync(string subject, byte[] data)
		{
			await _connection.RequestAsync(subject, data).ConfigureAwait(false);
		}

		public ValueTask<IDisposable> SubscribeAsync(NatsQueueScriptionSettings settings)
		{
			var subscription = _connection.SubscribeAsync(settings.Subject, settings.Queue, settings.EventHandler);

			return ValueTask.FromResult<IDisposable>(subscription);
		}

		public ValueTask<IDisposable> SubscribeAsync(NatsSubscriptionSettings settings)
		{
			var subscription = _connection.SubscribeAsync(settings.Subject, settings.EventHandler);

			return ValueTask.FromResult<IDisposable>(subscription);
		}
	}
}
