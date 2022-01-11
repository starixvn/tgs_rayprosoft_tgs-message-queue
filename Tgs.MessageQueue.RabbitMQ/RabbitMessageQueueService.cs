using RabbitMQ.Client;

namespace Tgs.MessageQueue.RabbitMQ;

internal class RabbitMessageQueueService : IMessageSender
{
	private readonly IModel _channel;
	private readonly string _exchangeName;

	public RabbitMessageQueueService(string exchangeName, IModel channel)
	{
		_channel = channel ?? throw new ArgumentNullException(nameof(channel));
		_exchangeName = exchangeName ?? throw new ArgumentNullException(nameof(exchangeName));
	}

	public ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
	{
		var properties = BuildBasicProperties(header);

		_channel.BasicPublish(_exchangeName, subject, properties, data);

		return ValueTask.CompletedTask;
	}

	public ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		=> throw new NotSupportedException();

	public ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
		=> throw new NotSupportedException();

	private IBasicProperties BuildBasicProperties(IEnumerable<MessageHeaderValue> header)
	{
		var properties = _channel.CreateBasicProperties();
		properties.Headers = header.ToDictionary(v => v.Name, v => (object?)v.Value);

		return properties;
	}
}
