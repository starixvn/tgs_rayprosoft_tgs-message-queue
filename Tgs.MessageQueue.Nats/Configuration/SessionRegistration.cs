using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class SessionRegistration<TMessageSession> : ISubscribeRegistration
	where TMessageSession : IMessageSession
{
	public string Subject { get; }

	public SessionRegistration(string subject)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));
		Subject = subject;
	}

	public ValueTask<IDisposable> SubscribeAsync(
		IMessageReceiver<NatsSubscriptionSettings> messageReceiver,
		IMessageReceiver<NatsQueueScriptionSettings> queueReceiver,
		IServiceProvider serviceProvider,
		ILogger logger,
		CancellationToken cancellationToken)
		=> messageReceiver.SubscribeAsync(new NatsSubscriptionSettings
		{
			Subject = Subject,
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
			.AddTag("handler", typeof(TMessageSession).Name));

		try
		{
			using var scope = dataInfo.ServiceProvider.CreateScope();
			var handler = ActivatorUtilities.CreateInstance<TMessageSession>(scope.ServiceProvider);
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
		TMessageSession handler,
		IMessageSender replySender)
	{
		try
		{
			var question = new NatsQuestion(
				dataInfo.Args.Message.Data,
				dataInfo.Args.Message.Reply,
				replySender);
			await handler
				.HandleAsync(question, dataInfo.CancellationToken)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");

			if (handler is IMessageExceptionProcessor exceptionProcessor)
				_ = await exceptionProcessor
					.HandleExceptionAsync(dataInfo.Args.Message.Data, ex, dataInfo.CancellationToken)
					.ConfigureAwait(false);
			else
				await replySender
					.PublishFailAsync(
						dataInfo.Args.Message.Reply,
						Encoding.UTF8.GetBytes(ex.ToString()),
						dataInfo.CancellationToken)
					.ConfigureAwait(false);
		}
	}
}
