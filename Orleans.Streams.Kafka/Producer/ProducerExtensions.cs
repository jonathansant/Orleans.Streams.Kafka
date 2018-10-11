using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Producer
{
	public static class ProducerExtensions
	{
		public static async Task Produce(this Producer<byte[], KafkaBatchContainer> producer, KafkaBatchContainer batch, TimeSpan timeout)
		{
			using (var tokenSource = new CancellationTokenSource(timeout))
			{
				await producer.ProduceAsync(
							batch.StreamNamespace,
							new Message<byte[], KafkaBatchContainer>
							{
								Key = batch.StreamGuid.ToByteArray(),
								Value = batch,
								Timestamp = new Timestamp(DateTimeOffset.UtcNow)
							}, 
						tokenSource.Token
					);
			}
		}
	}
}
