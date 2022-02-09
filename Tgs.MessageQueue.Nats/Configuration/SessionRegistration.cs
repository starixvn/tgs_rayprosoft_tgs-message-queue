using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class SessionRegistration<TMessageSession> : ISubscribeRegistration
	where TMessageSession : IMessageSession
{
	private readonly bool _isSession;

	public string Subject { get; }

	public SessionRegistration(string subject, bool isSession = true)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));
		Subject = subject;
		_isSession = isSession;
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

	private Question CreateQuestion(MessageDataInfo dataInfo, IMessageSender replySender, NatsMessageQueueService natsSernder)
	{
		var sessionReplySubjectValues = dataInfo.Args.Message.Header.GetValues(NatsMessageHeaderValueConsts.SessionReplySubjectKey);

		return _isSession
			? new NatsQuestion(
				dataInfo.Args.Message.Data,
				natsSernder,
				dataInfo.Args.Message.Reply)
			: new NatsAction(
				dataInfo.Args.Message.Data);
	}

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
			var natsSender = scope.ServiceProvider.GetRequiredService<NatsMessageQueueService>();
			var question = CreateQuestion(dataInfo, replySender, natsSender);

			await ProcessMessageAsync(
				question,
				handler,
				dataInfo.Logger,
				dataInfo.CancellationToken).ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			_ = (activity?.AddTag("error", true));
			dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");
		}
	}

	private async Task ProcessMessageAsync(
		Question question,
		TMessageSession handler,
		ILogger logger,
		CancellationToken cancellationToken)
	{
		try
		{
			await handler
				.HandleAsync(question, cancellationToken)
				.ConfigureAwait(false);
		}
		catch (Exception ex)
		{
			logger.LogError(ex, $"Handle {Subject} occur error.");

			if (handler is IMessageExceptionProcessor exceptionProcessor)
				_ = await exceptionProcessor
					.HandleExceptionAsync(question.Data, ex, cancellationToken)
					.ConfigureAwait(false);
			else if (question.CanResponse)
				await question
					.FailAsync(
						Encoding.UTF8.GetBytes(ex.ToString()),
						cancellationToken)
					.ConfigureAwait(false);
		}
	}
}
