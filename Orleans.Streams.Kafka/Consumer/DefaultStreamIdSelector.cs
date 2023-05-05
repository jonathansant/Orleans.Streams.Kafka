using Confluent.Kafka;
using System.Text;

namespace Orleans.Streams.Kafka.Consumer
{
	internal sealed class DefaultStreamIdSelector : IStreamIdSelector
	{
		public string GetStreamId(ConsumeResult<byte[], byte[]> result) => Encoding.UTF8.GetString(result.Message.Key);
	}
}
