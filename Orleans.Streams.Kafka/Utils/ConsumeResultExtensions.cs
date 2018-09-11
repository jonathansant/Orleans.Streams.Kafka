using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Streams.Kafka.Utils
{
	public static class ConsumeResultExtensions
	{
		public static KafkaBatchContainer ToBatchContainer(
			this ConsumeResult<byte[], byte[]> result,
			SerializationManager serializationManager,
			string streamNamespace
		)
		{
			var externalHeader = result.Headers.FirstOrDefault(header => header.Key == StreamProviderConstants.ExternalMessageHeader);
			var sequence = new EventSequenceTokenV2(result.Offset.Value);

			if (externalHeader != null)
			{
				var isExternal = BitConverter.ToBoolean(externalHeader.Value, 0);
				if (isExternal)
				{
					var key = Encoding.UTF8.GetString(result.Key);
					return new KafkaBatchContainer(
						StreamProviderUtils.GenerateStreamGuid(key),
						streamNamespace,
						new List<object> { Encoding.UTF8.GetString(result.Value) },
						null,
						isExternalBatch: true,
						streamSequenceToken: sequence
					);
				}
			}

			var batchContainer = serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(result.Value);
			batchContainer.SequenceToken = sequence;

			return batchContainer;
		}
	}
}
