using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MongoDB.Messaging;
using MongoDB.Messaging.Service;
using MongoDB.Messaging.Subscription;

namespace Tgs.MessageQueue.MongoDB.Configuration;

public class MongoDBMessageQueueBuilder
{
	private readonly Func<IServiceProvider, MongoMessageQueueBuilderOptions> _factory;
	private readonly List<QueueDeclare> _queueDeclares = new();
	private readonly IServiceCollection _services;
	private readonly List<SubscribeRegistration> _subscriptions = new();

	public string Name { get; }

	public MongoDBMessageQueueBuilder(
		IServiceCollection services,
		string name,
		Func<IServiceProvider, MongoMessageQueueBuilderOptions> factory)
	{
		if (string.IsNullOrWhiteSpace(name))
			throw new ArgumentException($"'{nameof(name)}' 不得為 Null 或空白字元。", nameof(name));
		_services = services ?? throw new ArgumentNullException(nameof(services));
		Name = name;
		_factory = factory ?? throw new ArgumentNullException(nameof(factory));
	}

	public MongoDBMessageQueueBuilder AddHandler<THandler>(string queueName, TimeSpan pollTime, int workers = 1)
		where THandler : class, IMessageHandler
	{
		_services.TryAddTransient<THandler>();

		_subscriptions.Add(new SubscribeRegistration
		{
			QueueName = queueName,
			PollTime = pollTime,
			Workers = workers,
			HandlerType = typeof(THandler)
		});

		return this;
	}

	public MongoDBMessageQueueBuilder DeclareQueue(
		string queueName,
		MessagePriority priority = MessagePriority.Normal,
		int retry = 0)
	{
		_queueDeclares.Add(new QueueDeclare(queueName, priority, retry));

		return this;
	}

	internal MessageService Build(IServiceProvider serviceProvider)
	{
		var manager = _factory(serviceProvider);
		manager.MessageQueue
			.Configure(builder =>
			{
				_ = builder.ConnectionString(manager.ConnectionString);

				foreach (var declare in _queueDeclares)
					_ = builder.Queue(
						queueBuilder => queueBuilder
							.Name(declare.Name)
							.Retry(declare.Retry)
							.Priority(declare.Priority));

				foreach (var registration in _subscriptions)
					_ = builder.Subscribe(
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

		managerService.Add(Name, manager.MessageQueue);

		return new MessageService(manager.MessageQueue.QueueManager);
	}
}
