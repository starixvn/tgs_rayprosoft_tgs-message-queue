using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.Nats.Configuration
{
	public class NatsMessageQueueConfiguration
	{
		private List<ISubscribeRegistration> _subscribeRegistrations = new List<ISubscribeRegistration>();

		public IServiceCollection Services { get; }

		public NatsMessageQueueConfiguration(IServiceCollection services)
		{
			Services = services;

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

		public NatsMessageQueueConfiguration ConfigQueueOptions(Action<NatsOptions, IServiceProvider> configure)
		{
			Services
				.AddOptions<NatsOptions>()
				.Configure(configure);

			return this;
		}
	}
}
