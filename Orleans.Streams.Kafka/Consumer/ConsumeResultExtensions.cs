using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using System.Collections.Generic;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;

namespace Orleans.Streams.Kafka.Consumer
{
	public static class ConsumeResultExtensions
	{
		public static KafkaBatchContainer ToBatchContainer(
			this ConsumeResult<byte[], byte[]> result,
			SerializationContext serializationContext,
			QueueProperties queueProperties,
			string streamId
		)
		{
			var sequence = new EventSequenceTokenV2(result.Offset.Value);

			if (queueProperties.IsExternal)
			{
				var message = serializationContext
					.ExternalStreamDeserializer
					.Deserialize(queueProperties, queueProperties.ExternalContractType, result.Message.Value);

				return new KafkaBatchContainer(
					StreamProviderUtils.GenerateStreamGuid(streamId),
					queueProperties.Namespace,
					new List<object> { message },
					null,
					sequence,
					result.TopicPartitionOffset
				);
			}

			var serializationManager = serializationContext.SerializationManager;
			var batchContainer = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(result.Message.Value);

			batchContainer.SequenceToken ??= sequence;
			batchContainer.TopicPartitionOffSet = result.TopicPartitionOffset;

			return batchContainer;
		}
	}
}
