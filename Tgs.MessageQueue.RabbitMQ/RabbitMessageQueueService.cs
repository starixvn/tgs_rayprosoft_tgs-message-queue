using RabbitMQ.Client;

namespace Tgs.MessageQueue.RabbitMQ;

internal class RabbitMessageQueueService : IMessageQueueService
{
	private readonly IModel _channel;
	private readonly string _exchangeName;

	public RabbitMessageQueueService(string exchangeName, IModel channel)
	{
		_channel = channel ?? throw new ArgumentNullException(nameof(channel));
		_exchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
	}

	public ValueTask<Answer> AskAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken)
		=> throw new NotSupportedException();

	public ValueTask PublishAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken)
	{
		using var activity = RabbitMQConnectionManager._RabbitMQActivitySource.StartActivity("RabbitMQ Publish");

		var appendHeaders = new List<MessageHeaderValue>(header);

		TraceContextPropagator.Inject(
			activity,
			appendHeaders,
			(headers, key, value) =>
			{
				if (!string.IsNullOrEmpty(value))
					headers.Add(new MessageHeaderValue(key, value));
			});

		var properties = BuildBasicProperties(appendHeaders);

		cancellationToken.ThrowIfCancellationRequested();
		_channel.BasicPublish(_exchangeName, subject, properties, data);

		return ValueTask.CompletedTask;
	}

	public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken)
		=> throw new NotSupportedException();

	public ValueTask SendAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken)
		=> throw new NotSupportedException();

	private IBasicProperties BuildBasicProperties(IEnumerable<MessageHeaderValue> header)
	{
		var properties = _channel.CreateBasicProperties();
		properties.Headers = header.ToDictionary(v => v.Name, v => (object?)v.Value);

		return properties;
	}
}
