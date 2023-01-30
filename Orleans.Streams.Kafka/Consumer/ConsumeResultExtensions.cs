using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using System.Collections.Generic;
using System.Runtime.Serialization;
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
				var key = Encoding.UTF8.GetString(result.Message.Key);
				var streamId = StreamId.Create(queueProperties.Namespace, key);

				var message = serializationContext
					.ExternalStreamDeserializer
					.Deserialize(queueProperties, queueProperties.ExternalContractType, result);

				return new KafkaBatchContainer(
					streamId,
					new List<object> { message },
					null,
					sequence,
					result.TopicPartitionOffset
				);
			}

			var serializationManager = serializationContext.SerializationManager;

			var serializedString = Encoding.UTF8.GetString(result.Message.Value);
			var batchContainer = serializationManager.Deserialize(typeof(KafkaBatchContainer), serializedString) as KafkaBatchContainer
				?? throw new SerializationException("Cannot deserialize consumed object as KafkaBatchContainer");

			batchContainer.SequenceToken ??= sequence;
			batchContainer.TopicPartitionOffSet = result.TopicPartitionOffset;

			return batchContainer;
		}
	}
}
