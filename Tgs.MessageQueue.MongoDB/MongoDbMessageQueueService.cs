using MongoMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB;

internal class MongoDbMessageQueueService : IMessageSender
{
	private readonly MongoMessageQueue _mongoMessageQueue;
	private readonly string _queueName;

	public MongoDbMessageQueueService(MongoMessageQueue mongoMessageQueue, string queueName)
	{
		_mongoMessageQueue = mongoMessageQueue ?? throw new ArgumentNullException(nameof(mongoMessageQueue));
		_queueName = queueName ?? throw new ArgumentNullException(nameof(queueName));
	}

	public async ValueTask PublishAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header)
	{
		var message = await _mongoMessageQueue.Publish(
			m => m
				.Queue(_queueName)
				.Data(new
				{
					Subject = subject,
					Data = data
				})
				.Correlation(Guid.NewGuid().ToString()));
	}

	public ValueTask<byte[]> RequestAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header) => throw new NotSupportedException();

	public ValueTask SendAsync(string subject, byte[] data, IEnumerable<MessageHeaderValue> header) => throw new NotSupportedException();
}
