using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using MongoDBMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB.Configuration;

public class MongoDBMessageQueueConfiguration
{
	public IServiceCollection Services { get; }

	public MongoDBMessageQueueConfiguration(MessageQueueConfiguration messageQueueConfiguration)
	{
		Services = messageQueueConfiguration.Services;
	}

	public MongoDBMessageQueueBuilder DefineMessageQueue(
		string name,
		Func<IServiceProvider, MongoDBMessageQueue> messageQueueFactory,
		Func<IServiceProvider, string> connectionStringFactory)
	{
		var builder = new MongoDBMessageQueueBuilder(
			Services,
			name,
			messageQueueFactory,
			connectionStringFactory);

		_ = Services.AddSingleton(builder);

		return builder;
	}
}
