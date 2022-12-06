using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DotNet.Globbing;
using Microsoft.Extensions.DependencyInjection;

namespace Tgs.MessageQueue.InProcess.Configuration;
internal class ReplyRegistration<THandler> : ISubscribeRegistration
	where THandler : class, IMessageHandler
{
	public Glob SubjectGlob { get; }

	public ReplyRegistration(Glob subjectGlob)
	{
		SubjectGlob = subjectGlob ?? throw new ArgumentNullException(nameof(subjectGlob));
	}

	public IMessageHandlerExecutor CreateExecutor(IServiceProvider serviceProvider)
	{
		return ActivatorUtilities.CreateInstance<MessageHandlerExecutor<THandler>>(serviceProvider);
	}
}
