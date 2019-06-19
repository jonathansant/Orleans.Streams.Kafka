using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Streams.Kafka.Consumer
{
	public static class ConsumeResultExtensions
	{
		public static KafkaBatchContainer ToBatchContainer(
			this ConsumeResult<byte[], byte[]> result,
			SerializationManager serializationManager,
			KafkaStreamOptions options,
			IExternalStreamSerializer serializer,
			string streamNamespace
		)
		{
			var externalHeader = result.Headers.FirstOrDefault(header => header.Key == options.ExternalMessageIdentifier);
			var sequence = new EventSequenceTokenV2(result.Offset.Value);

			if (externalHeader != null)
			{
				var isExternal = BitConverter.ToBoolean(externalHeader.GetValueBytes(), 0);
				if (isExternal)
				{
					var key = Encoding.UTF8.GetString(result.Key);
					return new KafkaBatchContainer(
						StreamProviderUtils.GenerateStreamGuid(key),
						streamNamespace,
						new List<object> { Encoding.UTF8.GetString(result.Value) },
						null,
						isExternalBatch: true,
						sequence,
						result.TopicPartitionOffset,
						serializer
					);
				}
			}

			var batchContainer = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(result.Value);

			if (batchContainer.SequenceToken == null)
				batchContainer.SequenceToken = sequence;

			batchContainer.TopicPartitionOffSet = result.TopicPartitionOffset;

			return batchContainer;
		}
	}
}
