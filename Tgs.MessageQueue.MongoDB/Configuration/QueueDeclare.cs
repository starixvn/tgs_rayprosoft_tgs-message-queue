using MongoDB.Messaging;

namespace Tgs.MessageQueue.MongoDB.Configuration;

internal record QueueDeclare(string Name, MessagePriority Priority = MessagePriority.Normal, int Retry = 0);
