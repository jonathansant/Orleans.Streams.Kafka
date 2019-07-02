using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Orleans.Streams.Utils.Serialization;
using System;

namespace Orleans.Streams.Kafka.Serialization
{
	public class AvroExternalStreamDeserializer<T> : IExternalStreamDeserializer<T>
	{
		private readonly SyncOverAsyncDeserializer<T> _deserializer;
		private readonly CachedSchemaRegistryClient _registry;
		private readonly string _topic;

		public AvroExternalStreamDeserializer(
			string schemaRegistryUrl,
			string topic
		)
		{
			_topic = topic;
			_registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryUrl });
			_deserializer = new SyncOverAsyncDeserializer<T>(new AvroDeserializer<T>(_registry));
		}

		public T Deserialize(byte[] data)
			=> _deserializer.Deserialize(
				new ReadOnlySpan<byte>(data),
				false,
				new SerializationContext(MessageComponentType.Value, _topic)
			);

		public void Dispose()
			=> _registry.Dispose();
	}
}
