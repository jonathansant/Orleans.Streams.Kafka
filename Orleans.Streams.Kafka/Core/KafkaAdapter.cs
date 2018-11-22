using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Producer;
using Orleans.Streams.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapter : IQueueAdapter, IDisposable
	{
		private readonly KafkaStreamOptions _options;
		private readonly IDictionary<string, QueueProperties> _queueProperties;
		private readonly SerializationManager _serializationManager;
		private readonly ILoggerFactory _loggerFactory;
		private readonly Producer<byte[], KafkaBatchContainer> _producer;
		private readonly ILogger<KafkaAdapter> _logger;

		public string Name { get; }
		public bool IsRewindable { get; } = false; // todo: provide way to pass sequence token (offset) so that we can rewind
		public StreamProviderDirection Direction { get; } = StreamProviderDirection.ReadWrite;

		public KafkaAdapter(
			string providerName,
			KafkaStreamOptions options,
			IDictionary<string, QueueProperties> queueProperties,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_options = options;
			_queueProperties = queueProperties;
			_serializationManager = serializationManager;
			_loggerFactory = loggerFactory;
			_logger = _loggerFactory.CreateLogger<KafkaAdapter>();

			Name = providerName;

			_producer = new Producer<byte[], KafkaBatchContainer>(
				options.ToProducerProperties(), 
				valueSerializer: (topic, container) => container.ToByteArray(_serializationManager)
			);
		}

		public async Task QueueMessageBatchAsync<T>(
			Guid streamGuid,
			string streamNamespace,
			IEnumerable<T> events,
			StreamSequenceToken token,
			Dictionary<string, object> requestContext
		)
		{
			try
			{
				var batch = new KafkaBatchContainer(
					streamGuid,
					streamNamespace,
					events.Cast<object>().ToList(),
					requestContext,
					false
				);

				await _producer.Produce(batch);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to publish message: streamNamespace: {namespace}, streamGuid: {guid}", streamNamespace, streamGuid);
				throw;
			}
		}

		public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
			=> new KafkaAdapterReceiver(
				_queueProperties[queueId.GetStringNamePrefix()], 
				_options, 
				_serializationManager, 
				_loggerFactory
			);

		public void Dispose()
			=> _producer.Dispose();
	}
}