using MongoDBMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB;

internal class QueueManagerService : Dictionary<string, MongoDBMessageQueue>
{
}
