using Tgs.MessageQueue.MongoDB.Configuration;
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

	public ValueTask<Answer> AskAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken)
		=> throw new NotSupportedException();

	public async ValueTask PublishAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken)
	{
		using var activity = MongoDBMessageQueueConfiguration._MongoActivitySource.StartActivity("Mongo Publish");

		cancellationToken.ThrowIfCancellationRequested();

		var message = await _mongoMessageQueue.Publish(
			m => m
				.Queue(_queueName)
				.Data(new MongoMessage
				{
					Subject = subject,
					Data = data.ToArray()
				})
				.Correlation(Guid.NewGuid().ToString()));
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
}
