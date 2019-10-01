using Confluent.Kafka;
using Orleans.Streams.Kafka.Core;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Consumer
{
	public static class ConsumerExtensions
	{
		public static Task Commit<TKey, TValue>(
			this IConsumer<TKey, TValue> consumer,
			params KafkaBatchContainer[] batches
		) => Task.Run(() => consumer.Commit(batches.Select(GetNextOffset)));

		private static TopicPartitionOffset GetNextOffset(KafkaBatchContainer msg)
		{
			var offset = new TopicPartitionOffset(
				msg.TopicPartitionOffSet.TopicPartition,
				msg.TopicPartitionOffSet.Offset.Value + 1
			);

			return offset;
		}
	}
}
