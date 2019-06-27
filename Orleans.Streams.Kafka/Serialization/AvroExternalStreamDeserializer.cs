using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Orleans.Streams.Utils.Serialization;
using System;

namespace Orleans.Streams.Kafka.Serialization
{
	public class AvroExternalStreamDeserializer : IExternalStreamDeserializer, IDisposable
	{
		private readonly AvroDeserializer<byte[]> _deserializer;
		private readonly CachedSchemaRegistryClient _registry;

		public AvroExternalStreamDeserializer()
		{
			_registry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = "https://dev-data.rivertech.dev/schema-registry" });
			_deserializer = new AvroDeserializer<byte[]>(_registry);
		}

		public T Deserialize<T>(object obj)
		{
			return (T)(new object());
		}

		public object Deserialize(Type type, object obj)
		{
			return null;
		}

		public void Dispose()
			=> _registry.Dispose();
	}
}
