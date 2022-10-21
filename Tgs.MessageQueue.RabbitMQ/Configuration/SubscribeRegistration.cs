using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.RabbitMQ.Configuration;

internal class SubscribeRegistration<THandler> : ISubscribeRegistration where THandler : IMessageHandler
{
	private static readonly Random _Random = new();
	private readonly bool _autoAck;
	private readonly int _dispatchConcurrency;

	public string Subject { get; }

	public SubscribeRegistration(string subject, bool autoAck, int dispatchConcurrency)
	{
		if (string.IsNullOrEmpty(subject))
			throw new ArgumentException($"'{nameof(subject)}' is not Null or Empty.", nameof(subject));

		Subject = subject;
		_autoAck = autoAck;
		_dispatchConcurrency = dispatchConcurrency;
	}

	public ValueTask<IDisposable> SubscribeAsync(
		IMessageReceiver<RabbitSubscriptionSettings> messageReceiver,
		IServiceProvider serviceProvider,
		ILogger logger,
		CancellationToken cancellationToken)
		=> messageReceiver.SubscribeAsync(new RabbitSubscriptionSettings
		{
			Subject = Subject,
			AutoAck = _autoAck,
			ConsumerDispatchConcurrency = _dispatchConcurrency,
			EventHandler = (model, args) =>
			{
				return HandleMessageAsync(new MessageDataInfo
				{
					Args = args,
					ServiceProvider = serviceProvider,
					Logger = logger,
					CancellationToken = cancellationToken,
					Channel = model
				});
			}
		});

	private async Task HandleMessageAsync(MessageDataInfo dataInfo)
	{
		using var activity = RabbitMQConnectionManager._RabbitMQActivitySource.StartActivity(Subject);

		if (activity is not null
			&& TraceContextPropagator.TryExtract(
				dataInfo.Args.BasicProperties.Headers,
				(headers, key) => (headers[key] as string)!,
				out var context))
		{
			_ = activity.SetParentId(context.TraceId, context.SpanId, context.TraceFlags);
			activity.TraceStateString = context.TraceState;
		}

		_ = (activity?.AddTag("mq", "RabbitMQ")
			.AddTag("handler", typeof(THandler).Name));

		try
		{
			using var scope = dataInfo.ServiceProvider.CreateScope();

			var handler = ActivatorUtilities.CreateInstance<THandler>(scope.ServiceProvider);

			await handler
				.HandleAsync(dataInfo.Args.Body.ToArray(), dataInfo.CancellationToken)
				.ConfigureAwait(false);

			if (!_autoAck)
				dataInfo.Channel.BasicAck(dataInfo.Args.DeliveryTag, false);
		}
		catch (Exception ex)
		{
			_ = (activity?.AddTag("error", true));
			dataInfo.Logger.LogError(ex, $"Handle {Subject} occur error.");

			if (!_autoAck)
			{
				await Task.Delay(_Random.Next(1, 5) * 1000).ConfigureAwait(false);

				dataInfo.Channel.BasicNack(dataInfo.Args.DeliveryTag, false, true);
			}
		}
	}
}
