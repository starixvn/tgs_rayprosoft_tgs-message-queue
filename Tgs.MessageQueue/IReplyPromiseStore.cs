namespace Tgs.MessageQueue;

public interface IReplyPromiseStore
{
	(Guid Id, Task<Answer> Promise) CreatePromise(CancellationToken cancellationToken);

	void SetCanceled(Guid id);

	void SetException(Guid id, Exception ex);

	void SetResult(Guid id, Answer answer);
}
