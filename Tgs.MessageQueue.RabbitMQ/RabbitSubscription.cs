using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Tgs.MessageQueue.RabbitMQ;

internal class RabbitSubscription : IDisposable
{
	private readonly IModel _channel;
	private readonly IConnection _connection;
	private readonly string _consumerTag;
	private readonly ILogger<RabbitSubscription> _logger;
	private readonly string _subject;
	private bool _disposedValue;

	public RabbitSubscription(
		string subject,
		IConnection connection,
		IModel channel,
		string consumerTag,
		ILogger<RabbitSubscription> logger)
	{
		_subject = subject;
		_connection = connection ?? throw new ArgumentNullException(nameof(connection));
		_channel = channel ?? throw new ArgumentNullException(nameof(channel));
		_consumerTag = consumerTag;
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));

		_connection.CallbackException += Connection_CallbackException;
		_connection.ConnectionBlocked += Connection_ConnectionBlocked;
		_connection.ConnectionShutdown += Connection_ConnectionShutdown;
		_connection.ConnectionUnblocked += Connection_ConnectionUnblocked;

		_channel.CallbackException += Connection_CallbackException;
		_channel.ModelShutdown += Channel_ModelShutdown;
	}

	public void Dispose()
	{
		// 請勿變更此程式碼。請將清除程式碼放入 'Dispose(bool disposing)' 方法
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}

	protected virtual void Dispose(bool disposing)
	{
		if (!_disposedValue)
		{
			if (disposing)
			{
				_channel.BasicCancel(_consumerTag);

				_connection.Dispose();
				_channel.Dispose();
			}

			_disposedValue = true;
		}
	}

	private void Channel_ModelShutdown(object? sender, ShutdownEventArgs e)
	{
		_logger.LogInformation($"Receiver({_subject}) ModelShutdownEvent");
	}

	private void Connection_CallbackException(object? sender, CallbackExceptionEventArgs e)
	{
		_logger.LogError(e.Exception, $"Receiver({_subject}) CallbackExceptionEvent");
	}

	private void Connection_ConnectionBlocked(object? sender, ConnectionBlockedEventArgs e)
	{
		_logger.LogInformation($"Receiver({_subject}) ConnectionBlockedEvent, Reason: {e.Reason}");
	}

	private void Connection_ConnectionShutdown(object? sender, ShutdownEventArgs e)
	{
		_logger.LogInformation($"Receiver({_subject}) ConnectionShutdownEvent");
	}

	private void Connection_ConnectionUnblocked(object? sender, EventArgs e)
	{
		_logger.LogInformation($"Receiver({_subject}) ConnectionUnblockedEvent");
	}
}
