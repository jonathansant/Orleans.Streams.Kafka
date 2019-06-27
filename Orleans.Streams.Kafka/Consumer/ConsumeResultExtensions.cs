using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using System.Collections.Generic;
using System.Text;
using Orleans.Streams.Utils.Serialization;

namespace Orleans.Streams.Kafka.Consumer
{
	public static class ConsumeResultExtensions
	{
		public static KafkaBatchContainer ToBatchContainer(
			this ConsumeResult<byte[], byte[]> result,
			SerializationManager serializationManager,
			QueueProperties queueProperties,
			IExternalStreamDeserializer deserializer
		)
		{
			var sequence = new EventSequenceTokenV2(result.Offset.Value);

			if (queueProperties.IsExternal)
			{
				var key = Encoding.UTF8.GetString(result.Key);
				return new KafkaBatchContainer(
					StreamProviderUtils.GenerateStreamGuid(key),
					queueProperties.Namespace,
					new List<object> { Encoding.UTF8.GetString(result.Value) },
					null,
					isExternalBatch: true,
					sequence,
					result.TopicPartitionOffset,
					deserializer
				);
			}

			var batchContainer = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(result.Value);

			if (batchContainer.SequenceToken == null)
				batchContainer.SequenceToken = sequence;

			batchContainer.TopicPartitionOffSet = result.TopicPartitionOffset;

			return batchContainer;
		}
	}
}
