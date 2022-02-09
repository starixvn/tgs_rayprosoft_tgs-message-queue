using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;

namespace Tgs.MessageQueue.Nats.Configuration;

public class NatsMessageQueueConfiguration
{
	internal static ActivitySource _NatsActivitySource = new("Tgs.MessageQueue.Nats");

	private readonly MessageQueueConfiguration _coreConfiguration;
	private readonly List<ISubscribeRegistration> _subscribeRegistrations = new();

	public IServiceCollection Services { get; }

	internal string? SessionReplySubject => _coreConfiguration.SessionReplySubject;

	public NatsMessageQueueConfiguration(IServiceCollection services)
	{
		Services = services ?? throw new ArgumentNullException(nameof(services));

		_coreConfiguration = services.AddMessageQueue();

		_ = Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
	}

	public NatsMessageQueueConfiguration(MessageQueueConfiguration coreConfiguration)
	{
		Services = coreConfiguration.Services;

		_coreConfiguration = coreConfiguration;

		_ = Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
	}

	public NatsMessageQueueConfiguration AddHandler<THandler>(string subject) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new SubscribeRegistration<THandler>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddHandler<THandler>(string subject, string group) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new QueueRegistration<THandler>(subject, group));

		return this;
	}

	public NatsMessageQueueConfiguration AddProcessor<TProcessor>(string subject) where TProcessor : IMessageProcessor
	{
		_subscribeRegistrations.Add(new ProcessRegistration<TProcessor>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddProcessor<TProcessor>(string subject, string queue) where TProcessor : IMessageProcessor
	{
		_subscribeRegistrations.Add(new QueueProcessRegistration<TProcessor>(subject, queue));

		return this;
	}

	public NatsMessageQueueConfiguration AddReplyHandler<THandler>(string subject) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new ReplyRegistration<THandler>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddReplyHandler<THandler>(string subject, string group) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new QueueReplyRegistration<THandler>(subject, group));

		return this;
	}

	public NatsMessageQueueConfiguration AddSession<TSession>(string subject) where TSession : IMessageSession
	{
		_subscribeRegistrations.Add(new SessionRegistration<TSession>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddSession<TSession>(string subject, string group) where TSession : IMessageSession
	{
		_subscribeRegistrations.Add(new QueueSessionRegistration<TSession>(subject, group));

		return this;
	}

	public NatsMessageQueueConfiguration ConfigQueueOptions(Action<NatsOptions, IServiceProvider> configure)
	{
		_ = Services
			.AddOptions<NatsOptions>()
			.Configure(configure);

		return this;
	}

	public NatsMessageQueueConfiguration SetSessionReplySubject(string subject)
	{
		if (string.IsNullOrWhiteSpace(subject))
			throw new ArgumentException($"'{nameof(subject)}' 不得為 Null 或空白字元。", nameof(subject));

		_subscribeRegistrations.Add(new SessionReplyRegistration(subject));
		_ = _coreConfiguration.RegisterSessionReplySubject(subject);

		return this;
	}
}
