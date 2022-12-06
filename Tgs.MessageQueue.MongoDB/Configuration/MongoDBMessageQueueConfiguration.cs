using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using MongoDBMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB.Configuration;

public class MongoDBMessageQueueConfiguration
{
	internal static ActivitySource _MongoActivitySource = new("Tgs.MessageQueue.Mongo");

	public IServiceCollection Services { get; }

	public MongoDBMessageQueueConfiguration(MessageQueueConfiguration messageQueueConfiguration)
	{
		Services = messageQueueConfiguration.Services;
	}

	public MongoDBMessageQueueBuilder DefineMessageQueue(
		string managerName,
		Func<IServiceProvider, MongoMessageQueueBuilderOptions> messageQueueFactory)
	{
		var builder = new MongoDBMessageQueueBuilder(
			Services,
			managerName,
			messageQueueFactory);

		_ = Services.AddSingleton(builder);

		return builder;
	}
}
