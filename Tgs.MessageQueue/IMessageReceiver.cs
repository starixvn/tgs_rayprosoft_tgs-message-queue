using System;
using System.Threading.Tasks;

namespace Tgs.MessageQueue;

public interface IMessageReceiver<TSubscriptionSettings>
{
	ValueTask<IDisposable> SubscribeAsync(TSubscriptionSettings settings);
}
