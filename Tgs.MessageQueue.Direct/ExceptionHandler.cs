using Microsoft.Extensions.Logging;

namespace Tgs.MessageQueue.Direct;

internal class ExceptionHandler
{
	private readonly Func<Exception, CancellationToken, Task> _handleException;
	private readonly ILogger<ExceptionHandler> _logger;

	public ExceptionHandler(
		Func<Exception, CancellationToken, Task> handleException,
		ILogger<ExceptionHandler> logger)
	{
		_handleException = handleException ?? throw new ArgumentNullException(nameof(handleException));
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
	}

	public Task HandleExceptionAsync(Exception ex, CancellationToken cancellationToken = default)
	{
		try
		{
			return _handleException(ex, cancellationToken);
		}
		catch (Exception handleEx)
		{
			_logger.LogCritical(handleEx, "Handle exception occur error.");

			return Task.CompletedTask;
		}
	}
}
