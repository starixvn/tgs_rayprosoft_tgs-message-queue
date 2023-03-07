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

	public NatsMessageQueueConfiguration AddHandler(Type handlerType, string subject)
	{
		var registrationType = typeof(SubscribeRegistration<>).MakeGenericType(handlerType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject)
			?? throw new InvalidOperationException($"Unable to create a registration for handler type {handlerType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddHandler<THandler>(string subject, string group) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new QueueRegistration<THandler>(subject, group));

		return this;
	}

	public NatsMessageQueueConfiguration AddHandler(Type handlerType, string subject, string group)
	{
		var registrationType = typeof(QueueRegistration<>).MakeGenericType(handlerType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject, group);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for handler type {handlerType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddProcessor<TProcessor>(string subject) where TProcessor : IMessageProcessor
	{
		_subscribeRegistrations.Add(new ProcessRegistration<TProcessor>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddProcessor(Type processorType, string subject)
	{
		var registrationType = typeof(ProcessRegistration<>).MakeGenericType(processorType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for processor type {processorType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddProcessor<TProcessor>(string subject, string queue) where TProcessor : IMessageProcessor
	{
		_subscribeRegistrations.Add(new QueueProcessRegistration<TProcessor>(subject, queue));

		return this;
	}

	public NatsMessageQueueConfiguration AddProcessor(Type processorType, string subject, string queue)
	{
		var registrationType = typeof(QueueProcessRegistration<>).MakeGenericType(processorType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject, queue);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for processor type {processorType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddReplyHandler<THandler>(string subject) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new ReplyRegistration<THandler>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddReplyHandler(Type handlerType, string subject)
	{
		var registrationType = typeof(ReplyRegistration<>).MakeGenericType(handlerType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for handler type {handlerType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddReplyHandler<THandler>(string subject, string group) where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(new QueueReplyRegistration<THandler>(subject, group));

		return this;
	}

	public NatsMessageQueueConfiguration AddReplyHandler(Type handlerType, string subject, string group)
	{
		var registrationType = typeof(QueueReplyRegistration<>).MakeGenericType(handlerType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject, group);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for handler type {handlerType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddSession<TSession>(string subject) where TSession : IMessageSession
	{
		_subscribeRegistrations.Add(new SessionRegistration<TSession>(subject));

		return this;
	}

	public NatsMessageQueueConfiguration AddSession(Type sessionType, string subject)
	{
		var registrationType = typeof(SessionRegistration<>).MakeGenericType(sessionType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for session type {sessionType.FullName}");

		_subscribeRegistrations.Add(registration);

		return this;
	}

	public NatsMessageQueueConfiguration AddSession<TSession>(string subject, string group) where TSession : IMessageSession
	{
		_subscribeRegistrations.Add(new QueueSessionRegistration<TSession>(subject, group));

		return this;
	}

	public NatsMessageQueueConfiguration AddSession(Type sessionType, string subject, string group)
	{
		var registrationType = typeof(QueueSessionRegistration<>).MakeGenericType(sessionType);
		var registration = (ISubscribeRegistration?)Activator.CreateInstance(registrationType, subject, group);

		if (registration == null)
			throw new InvalidOperationException($"Unable to create a registration for session type {sessionType.FullName}");

		_subscribeRegistrations.Add(registration);

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
