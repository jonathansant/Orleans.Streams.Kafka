using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Extensions
{
	public static class ConsumerExtensions
	{
		public static Task<ConsumeResult<TKey, TValue>> Poll<TKey, TValue>(this Consumer<TKey, TValue> consumer, TimeSpan timeout)
			=> Task.Run(() => consumer.Consume(timeout));

		public static Task Commit<TKey, TValue>(
			this Consumer<TKey, TValue> consumer,
			IEnumerable<KafkaBatchContainer> batches
		)
			=> Task.Run(() => consumer.Commit(batches.Select(msg => msg.TopicPartitionOffSet)));
	}
}
