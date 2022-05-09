﻿using Confluent.Kafka;
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
				var key = Encoding.UTF8.GetString(result.Message.Key);

				var headers = new List<KeyValuePair<string, byte[]>>();

				foreach (IHeader header in result.Message.Headers)
				{
					headers.Add(new(header.Key, header.GetValueBytes()));
				}

				var message = serializationContext
					.ExternalStreamDeserializer
					.Deserialize(queueProperties, queueProperties.ExternalContractType, result);

				return new KafkaBatchContainer(
					StreamProviderUtils.GenerateStreamGuid(key),
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
