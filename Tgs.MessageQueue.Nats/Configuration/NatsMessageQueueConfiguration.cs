using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue.Configuration;
using Tgs.MessageQueue.Exchanges;

namespace Tgs.MessageQueue.Nats.Configuration
{
	public class NatsMessageQueueConfiguration
	{
		private readonly MessageQueueConfiguration _coreConfiguration;
		private readonly AlwaysMatchMessageExchange<NatsMessageQueueService> _processAllExchange;
		private bool _isCustomExchange;
		private List<ISubscribeRegistration> _subscribeRegistrations = new List<ISubscribeRegistration>();

		public IServiceCollection Services { get; }

		public NatsMessageQueueConfiguration(IServiceCollection services)
		{
			Services = services ?? throw new ArgumentNullException(nameof(services));

			_coreConfiguration = services.AddMessageQueue();
			_processAllExchange = new AlwaysMatchMessageExchange<NatsMessageQueueService>();

			_coreConfiguration.AddExchange(_processAllExchange);
			_isCustomExchange = false;

			Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
		}

		public NatsMessageQueueConfiguration(MessageQueueConfiguration coreConfiguration)
		{
			Services = coreConfiguration.Services;

			_coreConfiguration = coreConfiguration;
			_processAllExchange = new AlwaysMatchMessageExchange<NatsMessageQueueService>();

			_coreConfiguration.AddExchange(_processAllExchange);
			_isCustomExchange = false;

			Services.AddSingleton<IEnumerable<ISubscribeRegistration>>(_subscribeRegistrations);
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

		public NatsMessageQueueConfiguration ConfigQueueOptions(Action<NatsOptions, IServiceProvider> configure)
		{
			Services
				.AddOptions<NatsOptions>()
				.Configure(configure);

			return this;
		}

		public NatsMessageQueueConfiguration ProcessAllSubjects()
		{
			if (!_isCustomExchange)
				return this;

			_coreConfiguration.AddExchange(_processAllExchange);

			return this;
		}

		public NatsMessageQueueConfiguration ProcessSubjectPattern(string glob)
		{
			if (!_isCustomExchange)
			{
				_coreConfiguration.RemoveExchange(_processAllExchange);

				_isCustomExchange = true;
			}

			_coreConfiguration.AddGlobPatternExchange<NatsMessageQueueService>(glob);

			return this;
		}
	}
}
