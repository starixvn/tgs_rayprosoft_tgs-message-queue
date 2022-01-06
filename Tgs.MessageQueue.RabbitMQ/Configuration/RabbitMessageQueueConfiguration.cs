using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.RabbitMQ.Exchanges;

namespace Tgs.MessageQueue.RabbitMQ.Configuration
{
	public class RabbitMessageQueueConfiguration
	{
		private readonly MessageQueueConfiguration _coreConfiguration;

		private List<ISubscribeRegistration> _subscribeRegistrations = new List<ISubscribeRegistration>();

		public IServiceCollection Services { get; }

		public RabbitMessageQueueConfiguration(IServiceCollection services)
		{
			Services = services ?? throw new ArgumentNullException(nameof(services));

			_coreConfiguration = services.AddMessageQueue();

			Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
		}

		public RabbitMessageQueueConfiguration(MessageQueueConfiguration coreConfiguration)
		{
			_coreConfiguration = coreConfiguration ?? throw new ArgumentNullException(nameof(coreConfiguration));

			Services = coreConfiguration.Services;

			Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
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
			Services
				.AddOptions<RabbitMQOptions>()
				.Configure(configure);

			return this;
		}

		[Obsolete]
		public RabbitMessageQueueConfiguration ProcessSubjectPattern(string glob, string exchangeName)
		{
			_coreConfiguration.AddExchange(new RabbitGlobMessageExchange(glob, exchangeName));

			return this;
		}
	}
}
