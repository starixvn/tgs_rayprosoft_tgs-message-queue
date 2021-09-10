using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.Exchanges;
using Tgs.MessageQueue.RabbitMQ.Exchanges;

namespace Tgs.MessageQueue.RabbitMQ.Configuration
{
	public class RabbitMessageQueueConfiguration
	{
		private readonly MessageQueueConfiguration _coreConfiguration;

		private readonly AlwayMatchMessageExchange<RabbitMessageQueueService> _processAllExchange;

		private bool _isCustomExchange;

		private List<ISubscribeRegistration> _subscribeRegistrations = new List<ISubscribeRegistration>();

		public IServiceCollection Services { get; }

		public RabbitMessageQueueConfiguration(IServiceCollection services)
		{
			Services = services ?? throw new ArgumentNullException(nameof(services));

			_coreConfiguration = services.AddMessageQueueCore();
			_processAllExchange = new AlwayMatchMessageExchange<RabbitMessageQueueService>();

			_coreConfiguration.AddExchange(_processAllExchange);
			_isCustomExchange = false;

			Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
		}

		public RabbitMessageQueueConfiguration AddHandler<THandler>(string subject, bool autoAck = true) where THandler : IMessageHandler
		{
			_subscribeRegistrations.Add(new SubscribeRegistration<THandler>(subject, autoAck));

			return this;
		}

		public RabbitMessageQueueConfiguration ConfigQueueOptions(Action<RabbitMQOptions, IServiceProvider> configure)
		{
			Services
				.AddOptions<RabbitMQOptions>()
				.Configure(configure);

			return this;
		}

		public RabbitMessageQueueConfiguration ProcessAllSubjects()
		{
			if (!_isCustomExchange)
				return this;

			_coreConfiguration.AddExchange(_processAllExchange);

			return this;
		}

		public RabbitMessageQueueConfiguration ProcessSubjectPattern(string glob, string exchaneName)
		{
			if (!_isCustomExchange)
			{
				_coreConfiguration.RemoveExchange(_processAllExchange);

				_isCustomExchange = true;
			}

			_coreConfiguration.AddExchange(new RabbitGlobMessageExchange(glob, exchaneName));

			return this;
		}
	}
}
