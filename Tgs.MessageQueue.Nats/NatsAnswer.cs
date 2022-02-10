namespace Tgs.MessageQueue.Nats;

internal record NatsAnswer : Answer
{
	private readonly IMessageSender _messageSender;

	public override bool CanResponse => !string.IsNullOrEmpty(ReplySubject);

	internal string? ReplySubject { get; }

	internal Guid? ReplyId { get; }

	internal NatsAnswer(
		ReadOnlyMemory<byte> responseData,
		IMessageSender messageSender,
		string? replySubject,
		Guid? replyId)
	{
		Result = responseData;
		ReplySubject = replySubject;
		ReplyId = replyId;
		_messageSender = messageSender ?? throw new ArgumentNullException(nameof(messageSender));
	}

	public override ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.AskAsync(
				ReplySubject!,
				data,
				header.Concat(new[] {
					new MessageHeaderValue(MessageHeaderValueConsts.SessionReplyKey, ReplyId.ToString()),
					new MessageHeaderValue(MessageHeaderValueConsts.SessionAskKey, string.Empty) }),
				cancellationToken)
			: throw new NatsReplySubjectNullException();

	public override ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.PublishAsync(
				ReplySubject!,
				data,
				header.Concat(new[] {
					new MessageHeaderValue(MessageHeaderValueConsts.SessionReplyKey, ReplyId.ToString())}),
				cancellationToken)
			: throw new NatsReplySubjectNullException();

	public override ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header, CancellationToken cancellationToken = default)
		=> CanResponse
			? _messageSender.PublishFailAsync(
				ReplySubject!,
				data,
				header.Concat(new[] {
					new MessageHeaderValue(MessageHeaderValueConsts.SessionReplyKey, ReplyId.ToString())}),
				cancellationToken: cancellationToken)
			: throw new NatsReplySubjectNullException();
}
