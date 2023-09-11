using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Messaging;
using MongoDB.Messaging.Subscription;
using Tgs.MessageQueue.MongoDB.Configuration;

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
		using var activity = TraceContextPropagator.TryExtract(
			processContext.Data<MongoMessage>(),
			(p, key) => key switch
			{
				TraceContextPropagator.TraceParent => p.TraceParent!,
				TraceContextPropagator.TraceState => p.TraceState!,
				_ => string.Empty
			},
			out var context)
			? MongoDBMessageQueueConfiguration._MongoActivitySource.StartActivity(
				processContext.Container.Name,
				ActivityKind.Server,
				context,
				tags: new[]
				{
					new KeyValuePair<string, object?>("mq", "MongoDB"),
					new KeyValuePair<string, object?>("handler", typeof(TReceiver).Name)
				})
			: MongoDBMessageQueueConfiguration._MongoActivitySource.StartActivity(
				ActivityKind.Server,
				name: processContext.Container.Name,
				tags: new[]
				{
					new KeyValuePair<string, object?>("mq", "MongoDB"),
					new KeyValuePair<string, object?>("handler", typeof(TReceiver).Name)
				});

		var payload = processContext.Data<MongoMessage>();

		try
		{
			Task.Run(async () =>
			{
				await using var scope = _serviceProvider.CreateAsyncScope();

				var receiver = scope.ServiceProvider.GetRequiredService<TReceiver>();

				await receiver.HandleAsync(payload!.Data).ConfigureAwait(false);
			}).GetAwaiter().GetResult();

			return MessageResult.Successful;
		}
		catch (Exception ex)
		{
			_ = (activity?.AddTag("error", true));
			_logger.LogError(ex, "Process fail.");

			Task.Run(async () =>
			{
				foreach (var handler in _serviceProvider.GetServices<ExceptionHandler>())
					await handler.HandleExceptionAsync(
						ex).ConfigureAwait(false);
			}).GetAwaiter().GetResult();

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
