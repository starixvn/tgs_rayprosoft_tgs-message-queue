using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Tgs.MessageQueue;
using Tgs.MessageQueue.Configuration;

namespace Microsoft.Extensions.DependencyInjection
{
	public static class ServiceCollectionExtensions
	{
		public static MessageQueueConfiguration AddMessageQueueCore(this IServiceCollection services)
		{
			var configuration = new MessageQueueConfiguration(services);

			services.AddTransient<IMessageSender>(
				sp => new MultiplexerMessageSender(sp, configuration.GetRegisterExchanges()));

			return configuration;
		}
	}
}
