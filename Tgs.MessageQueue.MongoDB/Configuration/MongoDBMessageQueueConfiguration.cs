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

	[Obsolete("這個方法將廢棄")]
	public MongoDBMessageQueueBuilder DefineMessageQueue(
		string name,
		Func<IServiceProvider, MongoDBMessageQueue> messageQueueFactory,
		Func<IServiceProvider, string> connectionStringFactory)
		=> DefineMessageQueue(
			name,
			sp => new MongoMessageQueueBuilderOptions
			{
				MessageQueue = messageQueueFactory(sp),
				ConnectionString = connectionStringFactory(sp)
			});

	public MongoDBMessageQueueBuilder DefineMessageQueue(
		string name,
		Func<IServiceProvider, MongoMessageQueueBuilderOptions> messageQueueFactory)
	{
		var builder = new MongoDBMessageQueueBuilder(
			Services,
			name,
			messageQueueFactory);

		_ = Services.AddSingleton(builder);

		return builder;
	}
}
