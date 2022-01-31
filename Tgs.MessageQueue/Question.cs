namespace Tgs.MessageQueue;

public abstract record Question
{
	public ReadOnlyMemory<byte> Data { get; protected set; }
	public abstract ValueTask<Answer> AskAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
	public abstract ValueTask CompleteAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
	public abstract ValueTask FailAsync(ReadOnlyMemory<byte> data, IEnumerable<MessageHeaderValue> header);
}
