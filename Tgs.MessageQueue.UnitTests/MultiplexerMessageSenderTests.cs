using NUnit.Framework;
using NSubstitute;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tgs.MessageQueue.UnitTests
{
	public class MultiplexerMessageSenderTests
	{
		[Test]
		public void Not_Register_Exchange_Will_Throw_Exception()
		{
			var fakeServiceProvider = Substitute.For<IServiceProvider>();

			var sut = new MultiplexerMessageSender(
				fakeServiceProvider,
				Array.Empty<IMessageExchange>());

			Assert.ThrowsAsync<MessageSenderNotFoundException>(
				async () => await sut.PublishAsync("test", Array.Empty<byte>()));
		}

		[Test]
		public async Task Exchange_Will_Select_First_Match_Sender()
		{
			var fakeServiceProvider = Substitute.For<IServiceProvider>();

			var exchange1 = Substitute.For<IMessageExchange>();
			var exchange2 = Substitute.For<IMessageExchange>();

			exchange1
				.Match(Arg.Any<string>(), Arg.Any<IEnumerable<MessageHeaderValue>>())
				.Returns(true);
			exchange2
				.Match(Arg.Any<string>(), Arg.Any<IEnumerable<MessageHeaderValue>>())
				.Returns(true);

			var sut = new MultiplexerMessageSender(
				fakeServiceProvider,
				new[] {
					exchange1,
					exchange2
				});

			await sut.PublishAsync("test", Array.Empty<byte>());

			exchange1.Received(1)
				.GetMessageSender(Arg.Any<IServiceProvider>());

			exchange2.Received(0)
				.GetMessageSender(Arg.Any<IServiceProvider>());
		}
	}
}