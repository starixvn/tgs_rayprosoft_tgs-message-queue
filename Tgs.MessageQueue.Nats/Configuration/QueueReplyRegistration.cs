using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class QueueReplyRegistration<THandler> : ISubscribeRegistration where THandler : IMessageHandler
{
	public string Queue { get; }

	public string Subject { get; }

	public QueueReplyRegistration(string subject, string queue)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));
		if (string.IsNullOrEmpty(queue))
			throw new ArgumentException($"'{nameof(queue)}' is not Null or Empty.", nameof(queue));
		Subject = subject;
		Queue = queue;
	}

	public ValueTask<IDisposable> SubscribeAsync(
		IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
		IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
		IServiceProvider serviceProvider,
		ILogger logger,
		CancellationToken cancellationToken)
		=> queueReceiver.SubscribeAsync(new NatsQueueScriptionSettings
		{
			Subject = Subject,
			Queue = Queue,
			EventHandler = (sender, args) =>
			{
				_ = HandleMessageAsync(new MessageDataInfo
				{
					Args = args,
					ServiceProvider = serviceProvider,
					Logger = logger,
					CancellationToken = cancellationToken
				});
			}
		});

	private async ValueTask HandleMessageAsync(MessageDataInfo dataInfo)
	{
		using var activity = NatsMessageQueueConfiguration._NatsActivitySource.StartActivity(Subject);

		_ = (activity?.AddTag("mq", "NATS")
			.AddTag("handler", typeof(THandler).Name));

		try
		{
			using var scope = dataInfo.ServiceProvider.CreateScope();
			var handler = ActivatorUtilities.CreateInstance<THandler>(scope.ServiceProvider);
			var replySender = scope.ServiceProvider.GetRequiredService<IMessageSender>();

			await ProcessMessageAsync(dataInfo, handler, replySender).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_ = (activity?.AddTag("error", true));
			dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");
		}
	}

	private async Task ProcessMessageAsync(
		MessageDataInfo dataInfo,
		THandler handler,
		IMessageSender replySender)
	{
		try
		{
			await handler
				.HandleAsync(dataInfo.Args.Message.Data, dataInfo.CancellationToken)
				.ConfigureAwait(false);

			await replySender
				.PublishAsync(dataInfo.Args.Message.Reply, Array.Empty<byte>(), dataInfo.CancellationToken)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");

			await replySender
				.PublishFailAsync(
					dataInfo.Args.Message.Reply,
					Encoding.UTF8.GetBytes(ex.ToString()),
					dataInfo.CancellationToken)
				.ConfigureAwait(false);
		}
	}
}
