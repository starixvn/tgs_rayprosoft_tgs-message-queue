using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.RabbitMQ.Exchanges;

namespace Tgs.MessageQueue.RabbitMQ.Configuration;

public class RabbitMessageQueueConfiguration
{
	internal static ActivitySource _RabbitActivitySource = new("Tgs.MessageQueue.RabbitMQ");

	private readonly MessageQueueConfiguration _coreConfiguration;

	private readonly List<ISubscribeRegistration> _subscribeRegistrations = new();

	public IServiceCollection Services { get; }

	public RabbitMessageQueueConfiguration(IServiceCollection services)
	{
		Services = services ?? throw new ArgumentNullException(nameof(services));

		_coreConfiguration = services.AddMessageQueue();

		_ = Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
	}

	public RabbitMessageQueueConfiguration(MessageQueueConfiguration coreConfiguration)
	{
		_coreConfiguration = coreConfiguration ?? throw new ArgumentNullException(nameof(coreConfiguration));

		Services = coreConfiguration.Services;

		_ = Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
	}

	public RabbitMessageQueueConfiguration AddHandler<THandler>(string queueName, bool autoAck = true, int dispatchConcurrency = 1)
		where THandler : IMessageHandler
	{
		_subscribeRegistrations.Add(
			new SubscribeRegistration<THandler>(queueName, autoAck, dispatchConcurrency));

		return this;
	}

	public RabbitMessageQueueConfiguration ConfigQueueOptions(Action<RabbitMQOptions, IServiceProvider> configure)
	{
		_ = Services
			.AddOptions<RabbitMQOptions>()
			.Configure(configure);

		return this;
	}

	[Obsolete("此方法已過時")]
	public RabbitMessageQueueConfiguration ProcessSubjectPattern(string glob, string exchangeName)
	{
		_ = _coreConfiguration.AddExchange(new RabbitGlobMessageExchange(glob, exchangeName));

		return this;
	}
}
