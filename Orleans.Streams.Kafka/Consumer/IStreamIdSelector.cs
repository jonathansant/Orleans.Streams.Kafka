using Confluent.Kafka;

namespace Orleans.Streams.Kafka.Consumer
{
	public interface IStreamIdSelector
	{
		string GetStreamId(ConsumeResult<byte[], byte[]> result);
	}
}
