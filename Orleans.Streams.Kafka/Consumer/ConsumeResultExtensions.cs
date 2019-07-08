using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using System.Collections.Generic;
using System.Text;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;

namespace Orleans.Streams.Kafka.Consumer
{
	public static class ConsumeResultExtensions
	{
		public static KafkaBatchContainer ToBatchContainer(
			this ConsumeResult<byte[], byte[]> result,
			SerializationContext serializationContext,
			QueueProperties queueProperties
		)
		{
			var sequence = new EventSequenceTokenV2(result.Offset.Value);

			if (queueProperties.IsExternal)
			{
				var key = Encoding.UTF8.GetString(result.Key);
				return new KafkaExternalBatchContainer(
					StreamProviderUtils.GenerateStreamGuid(key),
					queueProperties,
					new List<byte[]> { result.Value },
					sequence,
					result.TopicPartitionOffset,
					serializationContext.ExternalStreamDeserializer
				);
			}

			var serializationManager = serializationContext.SerializationManager;
			var batchContainer = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(result.Value);

			if (batchContainer.SequenceToken == null)
				batchContainer.SequenceToken = sequence;

			batchContainer.TopicPartitionOffSet = result.TopicPartitionOffset;

			return batchContainer;
		}
	}
}
