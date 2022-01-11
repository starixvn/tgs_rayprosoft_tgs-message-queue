using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Messaging;
using MongoDB.Messaging.Configuration;
using MongoDB.Messaging.Subscription;
using NSubstitute;
using Tgs.MessageQueue.MongoDB.Configuration;
using MongoDBMessage = MongoDB.Messaging.Message;
using MongoMessageQueue = MongoDB.Messaging.MessageQueue;

namespace Tgs.MessageQueue.MongoDB.IntegrationTests;

[TestClass]
public class DependencyInjectionTests
{
	[TestMethod]
	public void Can_Build_MessageService()
	{
		var services = new ServiceCollection();

		_ = services.AddMessageQueue()
			.AddMongoMessageQueue(configuration => configuration
				.DefineMessageQueue(
					"test",
					sp => MongoMessageQueue.Default,
					sp => "mongodb://localhost/Messaging")
				.DeclareQueue("queue")
				.AddHandler<StubMessageHandler>("test", TimeSpan.FromSeconds(1)));

		var serviceProvider = services.BuildServiceProvider();

		var sut = serviceProvider.GetRequiredService<MongoDBMessageQueueBuilder>();

		var actual = sut.Build(serviceProvider);
	}

	[TestMethod]
	public void Can_Create_Handler()
	{
		var services = new ServiceCollection();

		_ = services
			.AddLogging()
			.AddMessageQueue()
			.AddMongoMessageQueue(configuration => configuration
				.DefineMessageQueue(
					"test",
					sp => MongoMessageQueue.Default,
					sp => "mongodb://localhost/Messaging")
				.DeclareQueue("queue")
				.AddHandler<StubMessageHandler>("test", TimeSpan.FromSeconds(1)));

		var serviceProvider = services.BuildServiceProvider();

		var messageQueueBuilder = serviceProvider.GetRequiredService<MongoDBMessageQueueBuilder>();

		var sut = messageQueueBuilder.Build(serviceProvider);

		sut.Start();

		var actual = sut.Processors.First().Configuration.SubscriberFactory();

		Assert.IsNotNull(actual);
	}

	[TestMethod]
	public void Can_Receive_Message()
	{
		var services = new ServiceCollection();

		_ = services
			.AddLogging()
			.AddMessageQueue()
			.AddMongoMessageQueue(configuration => configuration
				.DefineMessageQueue(
					"test",
					sp => MongoMessageQueue.Default,
					sp => "mongodb://localhost/Messaging")
				.DeclareQueue("queue")
				.AddHandler<StubMessageHandler>("test", TimeSpan.FromSeconds(1)));

		var serviceProvider = services.BuildServiceProvider();

		var messageQueueBuilder = serviceProvider.GetRequiredService<MongoDBMessageQueueBuilder>();

		var manager = messageQueueBuilder.Build(serviceProvider);

		manager.Start();

		var sut = manager.Processors.First().Configuration.SubscriberFactory();

		var container = Substitute.For<IQueueContainer>();

		var actual = sut.Process(
			new ProcessContext(
				new MongoDBMessage
				{
					Data = new BsonDocument()
				},
				container));

		Assert.AreEqual(MessageResult.Successful, actual);
	}

	[TestMethod]
	public void Can_Resolve_MongoDBMessageQueueBuilders()
	{
		var services = new ServiceCollection();

		_ = services.AddMessageQueue()
			.AddMongoMessageQueue(configuration =>
			{
				_ = configuration.DefineMessageQueue(
					"test1",
					sp => MongoMessageQueue.Default,
					sp => "mongodb://localhost/Messaging");

				_ = configuration.DefineMessageQueue(
					"test2",
					sp => MongoMessageQueue.Default,
					sp => "mongodb://localhost/Messaging");
			});

		var sut = services.BuildServiceProvider();

		var actual = sut.GetServices<MongoDBMessageQueueBuilder>();

		Assert.AreEqual(2, actual.Count());
	}

	[TestMethod]
	public async Task Can_Send_Message()
	{
		var services = new ServiceCollection();

		var fakeManager = Substitute.For<IQueueManager>();

		var mongoMessageQueue = new MongoMessageQueue(fakeManager);

		_ = services
			.AddLogging()
			.AddMessageQueue()
			.AddMongoMessageQueue(configuration => configuration
				.DefineMessageQueue(
					"test",
					sp => mongoMessageQueue,
					sp => "mongodb://localhost/Messaging")
				.DeclareQueue("queue"))
			.AddMongoGlobPatternExchange("*", "test", "queue");

		var serviceProvider = services.BuildServiceProvider();

		foreach (var builder in serviceProvider.GetServices<MongoDBMessageQueueBuilder>())
			_ = builder.Build(serviceProvider);

		var sut = serviceProvider.GetRequiredService<IMessageSender>();

		await sut.PublishAsync("subject.test", new { Data = "Test" }.ToBson()).ConfigureAwait(false);
	}

	[TestMethod]
	public async Task When_Multiple_Exchange_Can_Send_Message()
	{
		var services = new ServiceCollection();

		var fakeManager = Substitute.For<IQueueManager>();

		var mongoMessageQueue = new MongoMessageQueue(fakeManager);

		_ = services
			.AddLogging()
			.AddMessageQueue()
			.AddMongoMessageQueue(configuration =>
			{
				_ = configuration
					.DefineMessageQueue(
						"test1",
						sp => mongoMessageQueue,
						sp => "mongodb://localhost/Messaging")
					.DeclareQueue("queue1");

				_ = configuration
					.DefineMessageQueue(
						"test2",
						sp => mongoMessageQueue,
						sp => "mongodb://localhost/Messaging")
					.DeclareQueue("queue2");
			})
			.AddMongoGlobPatternExchange("subject.test1", "test1", "queue1")
			.AddMongoGlobPatternExchange("subject.test2", "test2", "queue2");

		var serviceProvider = services.BuildServiceProvider();

		foreach (var builder in serviceProvider.GetServices<MongoDBMessageQueueBuilder>())
			_ = builder.Build(serviceProvider);

		var sut = serviceProvider.GetRequiredService<IMessageSender>();

		await sut.PublishAsync("subject.test1", new { Data = "Test" }.ToBson()).ConfigureAwait(false);
	}
}
