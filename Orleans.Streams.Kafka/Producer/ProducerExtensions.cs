using Confluent.Kafka;
using Orleans.Streams.Kafka.Core;
using System;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Producer
{
	public static class ProducerExtensions
	{
		public static Task Produce(this IProducer<string, KafkaBatchContainer> producer, KafkaBatchContainer batch)
			=> Task.Run(() => producer.ProduceAsync(
				batch.StreamId.GetNamespace(),
				new Message<string, KafkaBatchContainer>
				{
					Key = batch.StreamId.GetKeyAsString(),
					Value = batch,
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				}
			));
	}
}
