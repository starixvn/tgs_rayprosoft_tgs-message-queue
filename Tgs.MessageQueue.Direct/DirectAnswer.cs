namespace Tgs.MessageQueue.Direct;
public record DirectAnswer : Answer
{
	private readonly TaskCompletionSource<DirectAnswer> _taskCompletionSource = new();
	private readonly bool _canResponse;

	public DirectAnswer(ReadOnlyMemory<byte> data, bool canResponse)
	{
		Result = data;
		_canResponse = canResponse;
	}
	public override bool CanResponse => _canResponse;

	public override async ValueTask<Answer> AskAsync(
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		if (!CanResponse)
			throw new MessageCannotResponseException();

		using var activity = DirectDiagnostics.ActivitySource.StartActivity($"Answer Ask");

		_ = (activity?.AddTag("mq", "Direct")
			.AddTag("handler", typeof(DirectAnswer).Name));

		var answer = new DirectAnswer(data, true);

		_ = _taskCompletionSource.TrySetResult(answer);

		return await answer.GetAnwserAsync().ConfigureAwait(false);
	}

	public override ValueTask CompleteAsync(
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		if (!CanResponse)
			throw new MessageCannotResponseException();

		using var activity = DirectDiagnostics.ActivitySource.StartActivity($"Complete Answer");

		_ = (activity?.AddTag("mq", "Direct")
			.AddTag("handler", typeof(DirectAnswer).Name));

		var answer = new DirectAnswer(data, false);

		_ = _taskCompletionSource.TrySetResult(answer);

		return ValueTask.CompletedTask;
	}

	public override ValueTask FailAsync(
		ReadOnlyMemory<byte> data,
		IEnumerable<MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		if (!CanResponse)
			throw new MessageCannotResponseException();

		using var activity = DirectDiagnostics.ActivitySource.StartActivity($"Fail");

		_ = (activity?.AddTag("mq", "Direct")
			.AddTag("handler", typeof(DirectAnswer).Name));

		var answer = new DirectAnswer(data, false);

		_ = _taskCompletionSource.TrySetResult(answer);

		return ValueTask.CompletedTask;
	}

	internal async ValueTask<DirectAnswer> GetAnwserAsync()
		=> await _taskCompletionSource.Task.ConfigureAwait(false);
}
