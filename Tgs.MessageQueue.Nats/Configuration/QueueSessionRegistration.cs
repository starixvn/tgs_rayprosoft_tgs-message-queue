using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Nats.Configuration;

internal class QueueSessionRegistration<TMessageSession> : ISubscribeRegistration
	where TMessageSession : IMessageSession
{
	private readonly bool _isSession;

	public string Queue { get; }

	public string Subject { get; }

	public QueueSessionRegistration(string subject, string queue, bool isSession = true)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));
		if (string.IsNullOrEmpty(queue))
			throw new ArgumentException($"'{nameof(queue)}' is not Null or Empty.", nameof(queue));
		Subject = subject;
		Queue = queue;
		_isSession = isSession;
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
				}).AsTask();
			}
		});

	private Question CreateQuestion(MessageDataInfo dataInfo, INatsMessageQueueService natsSender)
		=> _isSession
			? new NatsQuestion(
				dataInfo.Args.Message.Data,
				natsSender,
				dataInfo.Args.Message.Reply)
			: new NatsAction(
				dataInfo.Args.Message.Data);

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

		_ = (activity?.AddTag("mq", "NATS")
			.AddTag("handler", typeof(TMessageSession).Name));

		try
		{
			using var scope = dataInfo.ServiceProvider.CreateScope();
			var handler = ActivatorUtilities.CreateInstance<TMessageSession>(scope.ServiceProvider);
			var natsSender = scope.ServiceProvider.GetRequiredService<INatsMessageQueueService>();
			var question = CreateQuestion(dataInfo, natsSender);

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
