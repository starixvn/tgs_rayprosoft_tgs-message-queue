using MongoDBMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB.Configuration;

public sealed class MongoMessageQueueBuilderOptions
{
	public string ConnectionString { get; init; } = default!;

	public MongoDBMessageQueue MessageQueue { get; init; } = default!;
}
