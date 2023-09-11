using Valhalla.MessageQueue;

namespace Tgs.MessageQueue.Compatible;

extern alias Tgs;

internal record AnswerWrapper : Tgs::Tgs.MessageQueue.Answer
{
	private readonly Answer _answer;

	public AnswerWrapper(Answer answer)
	{
		_answer = answer ?? throw new ArgumentNullException(nameof(answer));
	}
	public override bool CanResponse => _answer.CanResponse;

	public override async ValueTask<Tgs::Tgs.MessageQueue.Answer> AskAsync(
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs.Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		var response = await _answer.AskAsync(
			data,
			newHeader,
			cancellationToken).ConfigureAwait(false);

		return new AnswerWrapper(response);
	}

	public override ValueTask CompleteAsync(
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs::Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		return _answer.CompleteAsync(data, newHeader, cancellationToken);
	}

	public override ValueTask FailAsync(
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs::Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		return _answer.FailAsync(data, newHeader, cancellationToken);
	}
}
