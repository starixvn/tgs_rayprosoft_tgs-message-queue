namespace Tgs.MessageQueue.Nats;

internal record NatsAnswer : Answer
{
	private readonly string? _replySubject;
	private readonly Guid? _replyId;
	private readonly IMessageSender _messageSender;
	public override bool CanResponse => !string.IsNullOrEmpty(_replySubject);

	public override bool CanAsk => !string.IsNullOrEmpty(_replySubject);

	internal NatsAnswer(
		ReadOnlyMemory<byte> responseData,
		IMessageSender messageSender,
		string? replySubject,
		Guid? replyId)
	{
		Result = responseData;
		_replySubject = replySubject;
		_replyId = replyId;
		_messageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
	}

	public override ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanAsk
			? _messageSender.AskAsync(
				_replySubject!,
				data,
				header.Concat(new[] {
					new MessageHeaderValue(NatsMessageHeaderValueConsts.SessionReplyKey, _replyId.ToString()),
					new MessageHeaderValue(NatsMessageHeaderValueConsts.SessionAskKey, string.Empty) }),
				cancellationToken)
			: throw new NatsReplySubjectNullException();

	public override ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.PublishAsync(
				_replySubject!,
				data,
				header.Concat(new[] {
					new MessageHeaderValue(NatsMessageHeaderValueConsts.SessionReplyKey, _replyId.ToString())}),
				cancellationToken)
			: throw new NatsReplySubjectNullException();

	public override ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.PublishFailAsync(
				_replySubject!,
				data,
				header.Concat(new[] {
					new MessageHeaderValue(NatsMessageHeaderValueConsts.SessionReplyKey, _replyId.ToString())}),
				cancellationToken: cancellationToken)
			: throw new NatsReplySubjectNullException();
}
