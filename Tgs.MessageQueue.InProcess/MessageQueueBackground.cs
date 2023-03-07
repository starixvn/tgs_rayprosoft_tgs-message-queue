using System.Reactive;
using System.Reactive.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Tgs.MessageQueue.InProcess.Configuration;

namespace Tgs.MessageQueue.InProcess;

internal class MessageQueueBackground : BackgroundService
{
	private readonly IEventBus _eventBus;
	private readonly ILogger<MessageQueueBackground> _logger;
	private readonly IServiceProvider _serviceProvider;
	private readonly IEnumerable<ISubscribeRegistration> _subscribeRegistrations;

	public MessageQueueBackground(
		IEventBus eventBus,
		IEnumerable<ISubscribeRegistration> subscribeRegistrations,
		IServiceProvider serviceProvider,
		ILogger<MessageQueueBackground> logger)
	{
		_eventBus = eventBus ?? throw new ArgumentNullException(nameof(eventBus));
		_subscribeRegistrations = subscribeRegistrations ?? throw new ArgumentNullException(nameof(subscribeRegistrations));
		_serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
	}

	protected override Task ExecuteAsync(CancellationToken stoppingToken)
	{
		var subscription = _eventBus.MessageObservable
			.Do(_ => _logger.LogDebug("msg inbound"))
			.Select(msg => Observable.FromAsync(() => Task.Run(
				async () =>
				{
					using var activity = InProcessDiagnostics.ActivitySource.StartActivity(msg.Subject);

					await using var scope = _serviceProvider.CreateAsyncScope();

					bool TryFindSubscription(string subject, out IMessageHandlerExecutor executor)
					{
						foreach (var sub in _subscribeRegistrations)
							if (sub.SubjectGlob.IsMatch(subject))
							{
								executor = sub.CreateExecutor(scope.ServiceProvider);
								return true;
							}

						executor = default!;
						return false;
					}

					_logger.LogDebug("Process msg");

					if (TryFindSubscription(msg.Subject, out var handlerExecutor))
					{
						if (activity is not null
							&& TraceContextPropagator.TryExtract(
								msg.MessageHeaders.ToLookup(hv => hv.Name, hv => hv.Value),
								(header, key) => string.Join(", ", header[key]),
								out var context))
						{
							_ = activity.SetParentId(context.TraceId, context.SpanId, context.TraceFlags);
							activity.TraceStateString = context.TraceState;
						}

						_ = (activity?.AddTag("mq", "InProcess")
							.AddTag("handler", handlerExecutor.GetType().Name));

						await handlerExecutor.HandleAsync(
							msg,
							stoppingToken).ConfigureAwait(false);
					}
					else
					{
						_logger.LogError("Subject ({subject}) can't found handler or processor.", msg.Subject);

						msg.CompletionSource.SetException(new HandlerOrProcessorNotFoundException());
					}
				},
				stoppingToken))
				.Catch((Exception ex) => Observable.FromAsync(async () =>
				{
					_logger.LogError(ex, "Handle message occur error.");

					foreach (var handler in _serviceProvider.GetServices<ExceptionHandler>())
						await handler.HandleExceptionAsync(
							ex,
							stoppingToken).ConfigureAwait(false);
					return Unit.Default;
				})))
			.Merge()
			.Subscribe();

		_ = stoppingToken.Register(() => subscription.Dispose());

		return Task.CompletedTask;
	}
}
