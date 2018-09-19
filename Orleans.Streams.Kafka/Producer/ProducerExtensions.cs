using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Producer
{
	public static class ProducerExtensions
	{
		public static Task Produce(this Producer<byte[], KafkaBatchContainer> producer, KafkaBatchContainer batch, TimeSpan timeout)
			=> producer.ProduceAsync(
				batch.StreamNamespace,
				new Message<byte[], KafkaBatchContainer>
				{
					Key = batch.StreamGuid.ToByteArray(),
					Value = batch,
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				},
				new CancellationTokenSource(timeout).Token
			);
	}
}
