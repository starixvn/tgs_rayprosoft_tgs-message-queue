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
		private readonly RabbitMQOptions _options;
		private bool _disposedValue;
		public IModel MessageQueueChannel { get; }

		public RabbitMQConnectionManager(IOptions<RabbitMQOptions> optionsAccessor)
		{
			_options = optionsAccessor.Value;

			var factory = new ConnectionFactory()
			{
				Uri = _options.RabbitMQUrl,
				UserName = _options.UserName,
				Password = _options.Password,
				VirtualHost = _options.VirtualHost,
				DispatchConsumersAsync = true,
			};

			_connection = factory.CreateConnection();

			MessageQueueChannel = _connection.CreateModel();

			_options.OnSetupQueueAndExchange?.Invoke(MessageQueueChannel);
		}

		public void Dispose()
		{
			// 請勿變更此程式碼。請將清除程式碼放入 'Dispose(bool disposing)' 方法
			Dispose(disposing: true);

			GC.SuppressFinalize(this);
		}

		public ValueTask<IDisposable> SubscribeAsync(RabbitSubscriptionSettings settings)
		{
			var factory = new ConnectionFactory()
			{
				Uri = _options.RabbitMQUrl,
				UserName = _options.UserName,
				Password = _options.Password,
				VirtualHost = _options.VirtualHost,
				DispatchConsumersAsync = true,
				ConsumerDispatchConcurrency = settings.ConsumerDispatchConcurrency
			};

			var connection = factory.CreateConnection();
			var channel = connection.CreateModel();
			var consumer = new AsyncEventingBasicConsumer(MessageQueueChannel);
			consumer.Received += (sender, args) =>
			{
				return settings.EventHandler(channel, args);
			};

			var consumerTag = MessageQueueChannel.BasicConsume(
				queue: settings.Subject,
				autoAck: settings.AutoAck,
				consumer: consumer);

			return ValueTask.FromResult<IDisposable>(
				  new RabbitSubscription(connection, channel, consumerTag));
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
