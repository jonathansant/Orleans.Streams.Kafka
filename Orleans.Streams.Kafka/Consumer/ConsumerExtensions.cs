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
		) => Task.Run(() => consumer.Commit(batches.Select(msg => msg.TopicPartitionOffSet)));
	}
}
