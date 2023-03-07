using DotNet.Globbing;

namespace Tgs.MessageQueue.Direct.Configuration;

internal interface ISubscribeRegistration
{
	Glob SubjectGlob { get; }

	IMessageSender ResolveMessageSender(IServiceProvider serviceProvider);
}
