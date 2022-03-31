using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MongoDB.Messaging;
using MongoDB.Messaging.Subscription;

namespace Tgs.MessageQueue.MongoDB;

internal class MessageSubscriber<TReceiver> : IMessageSubscriber, IAsyncDisposable
	where TReceiver : class, IMessageHandler
{
	private static readonly ActivitySource _MongoDBActivitySource = new("Tgs.MessageQueue.MongoDB");

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
		using var activity = _MongoDBActivitySource.StartActivity(processContext.Container.Name);

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
			activity.SetParentId(context.TraceId, context.SpanId, context.TraceFlags);
			activity.TraceStateString = context.TraceState;
		}

		using var scope = _serviceProvider.CreateScope();

		try
		{
			var receiver = scope.ServiceProvider.GetRequiredService<TReceiver>();

#pragma warning disable VSTHRD002 // Avoid problematic synchronous waits
			Task.Run(async () => await receiver.HandleAsync(payload!.Data).ConfigureAwait(false)).Wait();
#pragma warning restore VSTHRD002 // Avoid problematic synchronous waits

			return MessageResult.Successful;
		}
		catch (Exception ex)
		{
			_ = (activity?.AddTag("error", true));
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

#pragma warning disable VSTHRD200 // Use "Async" suffix for async methods

	protected virtual ValueTask DisposeAsyncCore()
#pragma warning restore VSTHRD200 // Use "Async" suffix for async methods
	{
		return ValueTask.CompletedTask;
	}
}
