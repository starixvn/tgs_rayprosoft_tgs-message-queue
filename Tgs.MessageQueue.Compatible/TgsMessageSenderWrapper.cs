using Valhalla.MessageQueue;

namespace Tgs.MessageQueue.Compatible;

extern alias Tgs;

internal class TgsMessageSenderWrapper : Tgs::Tgs.MessageQueue.IMessageSender
{
	private readonly IMessageSender _phoenixToolKitsMessageSender;

	public TgsMessageSenderWrapper(IMessageSender messageSender)
	{
		_phoenixToolKitsMessageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
	}

	public async ValueTask<Tgs::Tgs.MessageQueue.Answer> AskAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs::Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		var answer = await _phoenixToolKitsMessageSender.AskAsync(
			subject,
			data,
			newHeader,
			cancellationToken).ConfigureAwait(false);

		return new AnswerWrapper(answer);
	}

	public ValueTask PublishAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs::Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		return _phoenixToolKitsMessageSender.PublishAsync(
			subject,
			data,
			newHeader,
			cancellationToken);
	}

	public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs::Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		return _phoenixToolKitsMessageSender.RequestAsync(
			subject,
			data,
			newHeader,
			cancellationToken);
	}

	public ValueTask SendAsync(
		string subject,
		ReadOnlyMemory<byte> data,
		IEnumerable<Tgs::Tgs.MessageQueue.MessageHeaderValue> header,
		CancellationToken cancellationToken = default)
	{
		var newHeader = header.Select(hv => new MessageHeaderValue(hv.Name, hv.Value));

		return _phoenixToolKitsMessageSender.SendAsync(
			subject,
			data,
			newHeader,
			cancellationToken);
	}
}
