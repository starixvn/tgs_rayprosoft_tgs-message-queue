using System;
using RabbitMQ.Client;

namespace Tgs.MessageQueue.RabbitMQ
{
	internal class RabbitSubscription : IDisposable
	{
		private readonly IModel _channel;

		private readonly string _consumerTag;

		private bool _disposedValue;

		public RabbitSubscription(IModel channel, string consumerTag)
		{
			_channel = channel ?? throw new ArgumentNullException(nameof(channel));
			_consumerTag = consumerTag;
		}

		public void Dispose()
		{
			// 請勿變更此程式碼。請將清除程式碼放入 'Dispose(bool disposing)' 方法
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (!_disposedValue)
			{
				if (disposing)
				{
					_channel.BasicCancel(_consumerTag);
				}
				_disposedValue = true;
			}
		}
	}
}
