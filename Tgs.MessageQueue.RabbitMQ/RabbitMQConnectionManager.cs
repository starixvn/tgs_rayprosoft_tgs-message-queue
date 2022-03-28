using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Tgs.MessageQueue.RabbitMQ;

internal class RabbitMQConnectionManager : IDisposable, IMessageReceiver<RabbitSubscriptionSettings>
{
	internal static readonly ActivitySource _RabbitMQActivitySource = new("Tgs.MessageQueue.RabbitMQ");

	private readonly IConnection _connection;
	private readonly ILogger<RabbitMQConnectionManager> _logger;
	private readonly RabbitMQOptions _options;
	private readonly IServiceProvider _serviceProvider;
	private bool _disposedValue;

	public IModel MessageQueueChannel { get; }

	public RabbitMQConnectionManager(
		IServiceProvider serviceProvider,
		IOptions<RabbitMQOptions> optionsAccessor,
		ILogger<RabbitMQConnectionManager> logger)
	{
		_options = optionsAccessor.Value;
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));

		var factory = new ConnectionFactory()
		{
			Uri = _options.RabbitMQUrl,
			UserName = _options.UserName,
			Password = _options.Password,
			VirtualHost = _options.VirtualHost,
			DispatchConsumersAsync = true
		};

		_connection = factory.CreateConnection();
		_connection.CallbackException += Connection_CallbackException;
		_connection.ConnectionBlocked += Connection_ConnectionBlocked;
		_connection.ConnectionShutdown += Connection_ConnectionShutdown;
		_connection.ConnectionUnblocked += Connection_ConnectionUnblocked;

		MessageQueueChannel = _connection.CreateModel();

		MessageQueueChannel.CallbackException += Connection_CallbackException;
		MessageQueueChannel.ModelShutdown += MessageQueueChannel_ModelShutdown;

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
		var consumer = new AsyncEventingBasicConsumer(channel);
		consumer.Received += (sender, args) => settings.EventHandler(channel, args);

		var consumerTag = channel.BasicConsume(
			queue: settings.Subject,
			autoAck: settings.AutoAck,
			consumer: consumer);

		return ValueTask.FromResult<IDisposable>(
			new RabbitSubscription(
				settings.Subject,
				connection,
				channel,
				consumerTag,
				_serviceProvider.GetRequiredService<ILogger<RabbitSubscription>>()));
	}

	protected virtual void Dispose(bool disposing)
	{
		if (!_disposedValue)
		{
			if (disposing)
			{
				_logger.LogWarning($"{nameof(RabbitMQConnectionManager)} disposing.");

				MessageQueueChannel?.Close();
				MessageQueueChannel?.Dispose();

				_connection.Close();
				_connection.Dispose();
			}

			_disposedValue = true;
		}
	}

	private void Connection_CallbackException(object? sender, CallbackExceptionEventArgs e)
	{
		_logger.LogError(e.Exception, "Sender CallbackExceptionEvent");
	}

	private void Connection_ConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
	{
		_logger.LogInformation($"Sender ConnectionBlockedEvent, Reason: {e.Reason}");
	}

	private void Connection_ConnectionShutdown(object? sender, ShutdownEventArgs e)
	{
		_logger.LogInformation("Sender ConnectionShutdownEvent");
	}

	private void Connection_ConnectionUnblocked(object? sender, EventArgs e)
	{
		_logger.LogInformation("Sender ConnectionUnblockedEvent");
	}

	private void MessageQueueChannel_ModelShutdown(object? sender, ShutdownEventArgs e)
	{
		_logger.LogInformation("Sender ModelShutdownEvent");
	}
}
