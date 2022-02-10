namespace Tgs.MessageQueue;

public interface IMessageSession
{
	ValueTask HandleAsync(Question question, CancellationToken cancellationToken = default);
}
