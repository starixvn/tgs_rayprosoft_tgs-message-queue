using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class SessionReplyRegistration : ISubscribeRegistration
{
	public string Subject { get; }

	public SessionReplyRegistration(string subject)
	{
		if (string.IsNullOrWhiteSpace(subject))
			throw new ArgumentException($"'{nameof(subject)}' 不得為 Null 或空白字元。", nameof(subject));
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
				}).AsTask();
			}
		});

	private async ValueTask HandleMessageAsync(MessageDataInfo dataInfo)
	{
		using var activity = NatsMessageQueueConfiguration._NatsActivitySource.StartActivity(Subject);

		if (dataInfo.Args.Message.HasHeaders
			&& activity is not null
			&& TraceContextPropagator.TryExtract(
				dataInfo.Args.Message.Header,
				(header, key) => header[key],
				out var context))
		{
			_ = activity.SetParentId(context.TraceId, context.SpanId, context.TraceFlags);
			activity.TraceStateString = context.TraceState;
		}

		_ = activity?.AddTag("mq", "NATS");

		try
		{
			await using var scope = dataInfo.ServiceProvider.CreateAsyncScope();
			var store = scope.ServiceProvider.GetRequiredService<IReplyPromiseStore>();
			var replySender = scope.ServiceProvider.GetRequiredService<IMessageSender>();
			var replyId = dataInfo.Args.Message.HasHeaders
				? dataInfo.Args.Message.Header.GetValues(MessageHeaderValueConsts.SessionReplyKey)?.FirstOrDefault()
				: null;

			dataInfo.Logger.LogInformation($"ReplyId: {replyId}");

			if (replyId is not null && Guid.TryParse(replyId, out var replyGuid))
			{
				var isFail = dataInfo.Args.Message.HasHeaders
					&& dataInfo.Args.Message.Header.GetValues(NatsMessageHeaderValueConsts.FailMessageHeaderValue.Name)?.Length > 0;
				var responseData = dataInfo.Args.Message.Data;
				var replySubject = dataInfo.Args.Message.HasHeaders
					? dataInfo.Args.Message.Header.GetValues(MessageHeaderValueConsts.SessionReplySubjectKey)?.FirstOrDefault()
					: null;
				var askId = dataInfo.Args.Message.HasHeaders
					? dataInfo.Args.Message.Header.GetValues(MessageHeaderValueConsts.SessionAskKey)?.FirstOrDefault()
					: null;

				dataInfo.Logger.LogInformation($"AskId: {askId}");

				if (isFail)
					store.SetException(replyGuid, new MessageProcessFailException(dataInfo.Args.Message.Data));
				else
					store.SetResult(
						replyGuid,
						new NatsAnswer(
							responseData,
							replySender,
							replySubject,
							askId != null && Guid.TryParse(askId, out var askGuid) ? askGuid : null));
			}
		}
		catch (Exception ex)
		{
			_ = activity?.AddTag("error", true);
			dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");
		}
	}
}
