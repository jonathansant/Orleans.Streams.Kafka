using Confluent.Kafka;
using Orleans.Streams.Kafka.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Extensions
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
					Timestamp = new Timestamp(DateTime.Now)
				},
				new CancellationTokenSource(timeout).Token
			);
	}
}
