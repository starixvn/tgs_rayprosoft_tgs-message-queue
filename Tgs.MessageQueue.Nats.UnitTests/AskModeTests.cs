using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NATS.Client;
using NSubstitute;
using NUnit.Framework;
using Tgs.MessageQueue.Nats.Configuration;

namespace Tgs.MessageQueue.Nats.UnitTests;

public class AskModeTests
{
	[Test]
	public async Task Ask_Should_Return_Answer()
	{
		var fakeNatsConnection = Substitute.For<IConnection>();
		var fakePromiseStore = Substitute.For<IReplyPromiseStore>();

		var sessionReplySubject = "test.reply";
		var sut = new NatsMessageQueueService(
			fakeNatsConnection,
			sessionReplySubject,
			fakePromiseStore,
			NullLogger<NatsMessageQueueService>.Instance);

		var askId = Guid.NewGuid();

		_ = fakeNatsConnection.RequestAsync(Arg.Any<Msg>(), Arg.Any<CancellationToken>())
			.Returns(callInfo =>
			{
				var msg = callInfo.Arg<Msg>();

				var header = new MsgHeader
				{
					{
						MessageHeaderValueConsts.SessionReplySubjectKey,
						msg.Header.GetValues(MessageHeaderValueConsts.SessionReplySubjectKey).First()
					},
					{
						MessageHeaderValueConsts.SessionAskKey,
						askId.ToString()
					}
				};

				return new Msg(msg.Subject, "reply", header, Array.Empty<byte>());
			});

		var actual = await sut.AskAsync("test", Array.Empty<byte>());

		Assert.IsNotNull(actual);
		Assert.IsInstanceOf<NatsAnswer>(actual);

		Assert.AreEqual(sessionReplySubject, (actual as NatsAnswer)?.ReplySubject);
		Assert.AreEqual(askId, (actual as NatsAnswer)?.ReplyId);
	}

	[Test]
	public async Task InternalAsk_Should_Return_Answer()
	{
		var fakeNatsConnection = Substitute.For<IConnection>();
		var fakePromiseStore = Substitute.For<IReplyPromiseStore>();

		var sessionReplySubject = "test.reply";
		var sut = new NatsMessageQueueService(
			fakeNatsConnection,
			sessionReplySubject,
			fakePromiseStore,
			NullLogger<NatsMessageQueueService>.Instance);

		var askId = Guid.NewGuid();

		var tcs = new TaskCompletionSource<Answer>();
		tcs.SetResult(new NatsAnswer(Array.Empty<byte>(), sut, sessionReplySubject, askId));

		_ = fakePromiseStore.CreatePromise(Arg.Any<CancellationToken>())
			.Returns((askId, tcs.Task));

		var actual = await sut.AskAsync(
			"test",
			Array.Empty<byte>(),
			new[] { new MessageHeaderValue(MessageHeaderValueConsts.SessionAskKey, string.Empty) },
			default);

		Assert.IsNotNull(actual);
		Assert.IsInstanceOf<NatsAnswer>(actual);

		Assert.AreEqual(sessionReplySubject, (actual as NatsAnswer)?.ReplySubject);
		Assert.AreEqual(askId, (actual as NatsAnswer)?.ReplyId);
	}

	[Test]
	public async Task ReplyHandler_Process()
	{
		var registration = new SessionReplyRegistration("test");
		var fakeMessageReceiver = Substitute.For<IMessageReceiver<NatsSubscriptionSettings>>();
		var fakeQueueReceiver = Substitute.For<IMessageReceiver<NatsQueueScriptionSettings>>();
		var fakePromiseStore = Substitute.For<IReplyPromiseStore>();

		var serviceCollection = new ServiceCollection();
		_ = serviceCollection
			.AddSingleton(fakePromiseStore)
			.AddSingleton(Substitute.For<IMessageSender>());

		var fakeServiceProvider = serviceCollection.BuildServiceProvider();

		EventHandler<MsgHandlerEventArgs>? sut = null;

		fakeMessageReceiver
			.When(fake => fake.SubscribeAsync(Arg.Any<NatsSubscriptionSettings>()))
			.Do(callInfo =>
			{
				var setting = callInfo.Arg<NatsSubscriptionSettings>();

				sut = setting.EventHandler;
			});

		_ = await registration.SubscribeAsync(
			fakeMessageReceiver,
			fakeQueueReceiver,
			fakeServiceProvider,
			NullLogger.Instance,
			default);

		var replyId = Guid.NewGuid();
		var askId = Guid.NewGuid();

		var eventArg = new MsgHandlerEventArgs(new Msg("test", new MsgHeader {
			{ MessageHeaderValueConsts.SessionReplyKey, replyId.ToString() },
			{ MessageHeaderValueConsts.SessionReplySubjectKey, "test"}
		}, Array.Empty<byte>()));

		var tcs = new TaskCompletionSource<Answer>();
		fakePromiseStore
			.When(fake => fake.SetResult(Arg.Is(replyId), Arg.Any<Answer>()))
			.Do(callInfo =>
			{
				var answer = callInfo.Arg<Answer>();

				tcs.SetResult(answer);
			});

		sut?.Invoke(null, eventArg);

		var actual = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
	}
}
