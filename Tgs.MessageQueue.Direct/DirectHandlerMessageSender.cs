using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Direct;

internal class DirectHandlerMessageSender<TMessageHandler> : IMessageSender
	where TMessageHandler : class, IMessageHandler
{
	private readonly ILogger<DirectHandlerMessageSender<TMessageHandler>> _logger;
	private readonly IServiceProvider _serviceProvider;

	public DirectHandlerMessageSender(
		IServiceProvider serviceProvider,
		ILogger<DirectHandlerMessageSender<TMessageHandler>> logger)
	{
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
	}

	public ValueTask<Answer> AskAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
		=> throw new NotSupportedException();

	public ValueTask PublishAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		using var activity = DirectDiagnostics.ActivitySource.StartActivity($"Direct Publish");

		_ = HandleMessageAsync(
			subject,
			data,
			cancellationToken).AsTask();

		return ValueTask.CompletedTask;
	}

	public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
		=> throw new NotSupportedException();

	public async ValueTask SendAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		using var activity = DirectDiagnostics.ActivitySource.StartActivity($"Direct Publish");

		await HandleMessageAsync(
			subject,
			data,
			cancellationToken).ConfigureAwait(false);
	}

	private async ValueTask HandleMessageAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		CancellationToken cancellationToken)
	{
		using var activity = DirectDiagnostics.ActivitySource.StartActivity(subject);

		_ = (activity?.AddTag("mq", "Direct")
			.AddTag("handler", typeof(TMessageHandler).Name));

		try
		{
			using var scope = _serviceProvider.CreateScope();
			var handler = ActivatorUtilities.CreateInstance<TMessageHandler>(scope.ServiceProvider);

			await handler.HandleAsync(data, cancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_ = (activity?.AddTag("error", true));
			_logger.LogError(ex, "Handle {subject} occur error.", subject);
		}

		return;
	}
}
