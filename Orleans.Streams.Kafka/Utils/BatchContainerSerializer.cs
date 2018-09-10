using Confluent.Kafka.Serialization;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using System;
using System.Collections.Generic;

namespace Orleans.Streams.Kafka.Utils
{
	public class BatchContainerSerializer : ISerializer<KafkaBatchContainer>
	{
		private readonly SerializationManager _serializationManager;

		public BatchContainerSerializer(SerializationManager serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public void Dispose()
			=> _serializationManager.Dispose();

		public byte[] Serialize(string topic, KafkaBatchContainer data)
			=> data.ToByteArray(_serializationManager);

		public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
			=> config;
	}

	public class BatchContainerDeserializer : IDeserializer<KafkaBatchContainer>
	{
		private readonly SerializationManager _serializationManager;

		public BatchContainerDeserializer(SerializationManager serializationManager)
		{
			_serializationManager = serializationManager;
		}

		public void Dispose()
			=> _serializationManager.Dispose();

		public KafkaBatchContainer Deserialize(string topic, ReadOnlySpan<byte> data, bool isNull)
			=> !isNull
				? _serializationManager.DeserializeFromByteArray<KafkaBatchContainer>(data.ToArray())
				: null;

		public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
			=> config;
	}
}
