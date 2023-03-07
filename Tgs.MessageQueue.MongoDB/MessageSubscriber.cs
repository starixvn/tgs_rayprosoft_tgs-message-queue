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

	[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "VSTHRD002:Avoid problematic synchronous waits", Justification = "<暫止>")]
	public MessageResult Process(ProcessContext processContext)
	{
		using var activity = MongoDBMessageQueueConfiguration._MongoActivitySource.StartActivity(processContext.Container.Name);

		_ = (activity?.AddTag("mq", "MongoDB")
			.AddTag("handler", typeof(TReceiver).Name));

		var payload = processContext.Data<MongoMessage>();

		if (activity is not null
			&& payload is not null
			&& !string.IsNullOrEmpty(payload.TraceParent)
			&& TraceContextPropagator.TryExtract(
				payload,
				(p, key) => key switch
				{
					TraceContextPropagator.TraceParent => p.TraceParent!,
					TraceContextPropagator.TraceState => p.TraceState!,
					_ => string.Empty
				},
				out var context))
		{
			_ = activity.SetParentId(context.TraceId, context.SpanId, context.TraceFlags);
			activity.TraceStateString = context.TraceState;
		}

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

#pragma warning disable VSTHRD200 // Use "Async" suffix for async methods

	protected virtual ValueTask DisposeAsyncCore()
#pragma warning restore VSTHRD200 // Use "Async" suffix for async methods
	{
		return ValueTask.CompletedTask;
	}
}
