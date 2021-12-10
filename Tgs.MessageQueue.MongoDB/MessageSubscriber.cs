using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Messaging;
using MongoDB.Messaging.Subscription;

namespace Tgs.MessageQueue.MongoDB;

internal class MessageSubscriber<TReceiver> : IMessageSubscriber, IAsyncDisposable
	where TReceiver : class, IMessageHandler
{
	private readonly ILogger<MessageSubscriber<TReceiver>> _logger;
	private readonly IServiceProvider _serviceProvider;
	private bool _disposedValue;

	public MessageSubscriber(IServiceProvider serviceProvider, ILogger<MessageSubscriber<TReceiver>> logger)
	{
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
	}

	public void Dispose()
	{
		// 請勿變更此程式碼。請將清除程式碼放入 'Dispose(bool disposing)' 方法
		Dispose(disposing: true);
		GC.SuppressFinalize(this);
	}

	public async ValueTask DisposeAsync()
	{
		await DisposeAsyncCore().ConfigureAwait(false);

		Dispose(false);

		GC.SuppressFinalize(this);
	}

	public MessageResult Process(ProcessContext processContext)
	{
		var payload = processContext.Data<MongoMessage>();

		using var scope = _serviceProvider.CreateScope();

		try
		{
			var receiver = scope.ServiceProvider.GetRequiredService<TReceiver>();

			Task.Run(async () => await receiver.HandleAsync(payload.Data).ConfigureAwait(false)).Wait();

			return MessageResult.Successful;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Process fail.");

			return MessageResult.Error;
		}
	}

	protected virtual void Dispose(bool disposing)
	{
		if (!_disposedValue)
		{
			if (disposing)
			{
			}

			_disposedValue = true;
		}
	}

	protected virtual ValueTask DisposeAsyncCore()
	{
		return ValueTask.CompletedTask;
	}
}
