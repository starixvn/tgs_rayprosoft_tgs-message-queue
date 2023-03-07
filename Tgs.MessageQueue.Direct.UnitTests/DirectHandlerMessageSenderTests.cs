using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace Tgs.MessageQueue.Direct.UnitTests;
public class DirectHandlerMessageSenderTests
{
	[Fact]
	public async Task Publish_Message()
	{
		// Arrange
		var fakeServiceProvider = Substitute.For<IServiceProvider>();
		var fakeHandler = Substitute.For<IMessageHandler>();

		var sut = new DirectHandlerMessageSender<IMessageHandler>(
			fakeServiceProvider,
			NullLogger<DirectHandlerMessageSender<IMessageHandler>>.Instance);

		// Act
		await sut.PublishAsync("test", Array.Empty<byte>());

		// Assert

	}
}
