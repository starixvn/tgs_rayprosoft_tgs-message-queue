using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using MongoDBMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB.Configuration;

public class MongoDBMessageQueueConfiguration
{
	private readonly MessageQueueConfiguration _messageQueueConfiguration;

	public IServiceCollection Services { get; }

	public MongoDBMessageQueueConfiguration(MessageQueueConfiguration messageQueueConfiguration)
	{
		Services = messageQueueConfiguration.Services;
		_messageQueueConfiguration = messageQueueConfiguration ?? throw new ArgumentNullException(nameof(messageQueueConfiguration));
	}

	public MongoDBMessageQueueBuilder DefineMessageQueue(
		string name,
		Func<IServiceProvider, MongoDBMessageQueue> messageQueueFactory,
		Func<IServiceProvider, string> connectionStringFactory)
	{
		var builder = new MongoDBMessageQueueBuilder(name, messageQueueFactory, connectionStringFactory);

		Services.AddSingleton(builder);

		return builder;
	}
}
