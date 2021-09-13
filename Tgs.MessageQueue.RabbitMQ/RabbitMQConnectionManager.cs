using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Tgs.MessageQueue.RabbitMQ
{
	internal class RabbitMQConnectionManager : IDisposable, IMessageReceiver<RabbitSubscriptionSettings>
	{
		private readonly IConnection _connection;
		private readonly ConnectionFactory _factory;
		private bool _disposedValue;
		public IModel MessageQueueChannel { get; }

		public RabbitMQConnectionManager(IOptions<RabbitMQOptions> optionsAccessor)
		{
			var options = optionsAccessor.Value;

			_factory = new ConnectionFactory()
			{
				Uri = options.RabbitMQUrl,
				UserName = options.UserName,
				Password = options.Password,
				VirtualHost = options.VirtualHost,
				DispatchConsumersAsync = true
			};

			_connection = _factory.CreateConnection();

			MessageQueueChannel = _connection.CreateModel();

			options.OnSetupQueueAndExchange?.Invoke(MessageQueueChannel);
		}

		public void Dispose()
		{
			// 請勿變更此程式碼。請將清除程式碼放入 'Dispose(bool disposing)' 方法
			Dispose(disposing: true);

			GC.SuppressFinalize(this);
		}

		public ValueTask<IDisposable> SubscribeAsync(RabbitSubscriptionSettings settings)
		{
			var consumer = new AsyncEventingBasicConsumer(MessageQueueChannel);
			consumer.Received += settings.EventHandler;

			var consumerTag = MessageQueueChannel.BasicConsume(
				queue: settings.Subject,
				autoAck: settings.AutoAck,
				consumer: consumer);

			return ValueTask.FromResult<IDisposable>(new RabbitSubscription(MessageQueueChannel, consumerTag));
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!_disposedValue)
			{
				if (disposing)
				{
					MessageQueueChannel?.Close();
					MessageQueueChannel?.Dispose();

					_connection.Close();
					_connection.Dispose();
				}

				_disposedValue = true;
			}
		}
	}
}
