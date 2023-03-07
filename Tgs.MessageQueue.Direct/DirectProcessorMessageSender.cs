using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Direct;

internal class DirectProcessorMessageSender<TMessageProcessor> : IMessageSender
	where TMessageProcessor : class, IMessageProcessor
{
	private readonly IServiceProvider _serviceProvider;

	public DirectProcessorMessageSender(
		IServiceProvider serviceProvider)
	{
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
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
		=> throw new NotSupportedException();

	public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		using var requestActivity = DirectDiagnostics.ActivitySource.StartActivity($"Direct Request");

		using var activity = DirectDiagnostics.ActivitySource.StartActivity(subject);

		_ = (activity?.AddTag("mq", "Direct")
			.AddTag("handler", typeof(TMessageProcessor).Name));

		await using var scope = _serviceProvider.CreateAsyncScope();
		var handler = ActivatorUtilities.CreateInstance<TMessageProcessor>(scope.ServiceProvider);

		return await handler.HandleAsync(
			data,
			cancellationToken).ConfigureAwait(false);
	}

	public ValueTask SendAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
		=> throw new NotImplementedException();
}
