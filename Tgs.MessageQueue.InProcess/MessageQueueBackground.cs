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
	private readonly IServiceProvider _serviceProvider;
	private readonly ILogger<MessageQueueBackground> _logger;
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
		var subscription = _eventBus.BucketChangedObservable
			.Select(msg => Observable.FromAsync(() => Task.Run(
				async () =>
				{
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

					if (TryFindSubscription(msg.Subject, out var handlerExecutor))
						await handlerExecutor.HandleAsync(
							msg,
							stoppingToken).ConfigureAwait(false);
					else
						msg.CompletionSource.SetException(new HandlerOrProcessorNotFoundException());
				},
				stoppingToken))
				.Catch<Unit, Exception>(ex =>
				{
					_logger.LogError(ex, "Handle message occur error.");

					return Observable.Empty<Unit>();
				}))
			.Subscribe();

		_ = stoppingToken.Register(() => subscription.Dispose());

		return Task.CompletedTask;
	}
}
