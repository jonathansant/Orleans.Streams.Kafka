using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;
using Orleans.Streams.Kafka.Utils;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapter : IQueueAdapter, IDisposable
	{
		//		private readonly QueueProperties _queueProperties;
		private readonly IStreamQueueMapper _streamQueueMapper;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly ILoggerFactory _loggerFactory;
		private readonly Producer<byte[], KafkaBatchContainer> _producer;

		public string Name { get; }
		public bool IsRewindable { get; } = true; // todo: provide way to pass sequence token (offset) so that we can rewind
		public StreamProviderDirection Direction { get; } = StreamProviderDirection.ReadWrite;

		public KafkaAdapter(
			string providerName,
			//QueueProperties queueProperties,
			IStreamQueueMapper streamQueueMapper,
			KafkaStreamOptions options, // todo: maybe pass producer properties immediately?
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			//_queueProperties = queueProperties;
			_streamQueueMapper = streamQueueMapper;
			_options = options;
			_serializationManager = serializationManager;
			_loggerFactory = loggerFactory;

			Name = providerName;

			_producer = new Producer<byte[], KafkaBatchContainer>(
				options.ToProducerProperties(), // todo: investigate other constructor options
				new ByteArraySerializer(), 
				new BatchContainerSerializer(serializationManager)
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
			//var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
			//var partitionId = (int)queueId.GetNumericId();

			try
			{
				var batch = new KafkaBatchContainer(
					streamGuid,
					streamNamespace,
					events.Cast<object>().ToList(),
					requestContext,
					false // todo: to get the if message id external
				);

//				var key = streamGuid.ToByteArray();
//				var value = batch.ToByteArray(_serializationManager);

//				var message = await _producer.ProduceAsync(
//					queueId.GetStringNamePrefix(),
//					key,
//					0,
//					key.Length,
//					value,
//					0,
//					value.Length,
//					partitionId,
//					true // todo: true?
//				);

				var message = await _producer.ProduceAsync(
					streamNamespace,
					new Message<byte[], KafkaBatchContainer>
					{
						Value = batch,
						Key = streamGuid.ToByteArray()
					} // todo: consider adding a cancellation token 
				);

				// todo: log message sent
			}
			catch (Exception ex)
			{
				// todo: log
			}
		}

		public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
			=> new KafkaAdapterReceiver(queueId, _options, _serializationManager, _loggerFactory);

		public void Dispose()
		{
			_producer.Dispose(); // todo: is this enough?
		}
	}
}