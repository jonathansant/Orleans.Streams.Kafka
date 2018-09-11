using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Utils
{
	public static class ConsumerExtensions
	{
		public static Task<ConsumeResult<TKey, TValue>> Poll<TKey, TValue>(this Consumer<TKey, TValue> consumer, TimeSpan timeout)
			=> Task.Run(() => consumer.Consume(timeout));

		public static Task Commit<TKey, TValue>(
			this Consumer<TKey, TValue> consumer,
			IEnumerable<ConsumeResult<TKey, TValue>> consumeResults
		)
			=> Task.Run(() => consumer.Commit(consumeResults.Select(msg => msg.TopicPartitionOffset)));
	}
}
