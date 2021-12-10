using Microsoft.Extensions.Hosting;
using MongoDB.Messaging.Service;
using Tgs.MessageQueue.MongoDB.Configuration;

namespace Tgs.MessageQueue.MongoDB;

internal class MessageQueueBackground : BackgroundService
{
	private readonly MessageService[] _queueServices;

	public MessageQueueBackground(IEnumerable<MongoDBMessageQueueBuilder> builders, IServiceProvider serviceProvider)
	{
		_queueServices = builders
			.Select(builder => builder.Build(serviceProvider))
			.ToArray();
	}

	protected override Task ExecuteAsync(CancellationToken stoppingToken)
	{
		foreach (var svc in _queueServices)
			svc.Start();

		stoppingToken.Register(() =>
		{
			foreach (var svc in _queueServices)
				svc.Stop();
		});

		return Task.CompletedTask;
	}
}
