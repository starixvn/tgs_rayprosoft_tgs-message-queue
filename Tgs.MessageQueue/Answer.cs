namespace Tgs.MessageQueue;

public abstract record Answer
{
	public ReadOnlyMemory<byte> Result { get; protected set; }
	public abstract ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
	public abstract ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
	public abstract ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
}
