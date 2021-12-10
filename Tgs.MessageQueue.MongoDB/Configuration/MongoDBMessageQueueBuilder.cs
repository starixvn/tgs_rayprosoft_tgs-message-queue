using Microsoft.Extensions.DependencyInjection;
using MongoDB.Messaging.Service;
using MongoDB.Messaging.Subscription;
using MongoDBMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB.Configuration;

public class MongoDBMessageQueueBuilder
{
	private readonly Func<IServiceProvider, MongoDBMessageQueue> _factory;
	private readonly Func<IServiceProvider, string> _mongoConnectionStringFactory;
	private List<string> _queueDeclares = new List<string>();
	private List<SubscribeRegistration> _subscriptions = new List<SubscribeRegistration>();

	public string Name { get; }

	public MongoDBMessageQueueBuilder(
		string name,
		Func<IServiceProvider, MongoDBMessageQueue> factory,
		Func<IServiceProvider, string> mongoConnectionStringFactory)
	{
		if (string.IsNullOrWhiteSpace(name))
			throw new ArgumentException($"'{nameof(name)}' 不得為 Null 或空白字元。", nameof(name));

		Name = name;
		_factory = factory ?? throw new ArgumentNullException(nameof(factory));
		_mongoConnectionStringFactory = mongoConnectionStringFactory ?? throw new ArgumentNullException(nameof(mongoConnectionStringFactory));
	}

	public MongoDBMessageQueueBuilder AddHandler<THandler>(string queueName, TimeSpan pollTime, int workers = 1)
		where THandler : class, IMessageHandler
	{
		_subscriptions.Add(new SubscribeRegistration
		{
			QueueName = queueName,
			PollTime = pollTime,
			Workers = workers,
			HandlerType = typeof(THandler)
		});

		return this;
	}

	public MongoDBMessageQueueBuilder DeclareQueue(string queueName)
	{
		_queueDeclares.Add(queueName);

		return this;
	}

	internal MessageService Build(IServiceProvider serviceProvider)
	{
		var manager = _factory(serviceProvider);
		manager
			.Configure(builder =>
			{
				builder.ConnectionString(_mongoConnectionStringFactory(serviceProvider));

				foreach (var queueName in _queueDeclares)
					builder.Queue(
						queueBuilder => queueBuilder
							.Name(queueName));

				foreach (var registration in _subscriptions)
					builder.Subscribe(
						subBuilder => subBuilder
							.Queue(registration.QueueName)
							.Handler(() =>
							{
								var subscriberType = typeof(MessageSubscriber<>);

								var handlerType = subscriberType.MakeGenericType(registration.HandlerType);

								return serviceProvider.GetRequiredService(handlerType) as IMessageSubscriber;
							})
							.PollTime(registration.PollTime)
							.Workers(registration.Workers)
							.Trigger());
			});

		var managerService = serviceProvider.GetRequiredService<QueueManagerService>();

		managerService.Add(Name, manager);

		return new MessageService(manager.QueueManager);
	}
}
