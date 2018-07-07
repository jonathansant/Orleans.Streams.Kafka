using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Utils;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapter : IQueueAdapter, IDisposable
	{
		private readonly QueueProperties _queueProperties;
		private readonly IStreamQueueMapper _streamQueueMapper;
		private readonly SerializationManager _serializationManager;
		private readonly Producer _producer;

		public string Name { get; }
		public bool IsRewindable { get; } = true;
		public StreamProviderDirection Direction { get; } = StreamProviderDirection.ReadWrite;

		public KafkaAdapter(
			string providerName, 
			QueueProperties queueProperties,
			IStreamQueueMapper streamQueueMapper,
			KafkaStreamOptions options, // todo: maybe pass producer properties immediately?
			SerializationManager serializationManager
		)
		{
			_queueProperties = queueProperties;
			_streamQueueMapper = streamQueueMapper;
			_serializationManager = serializationManager;
			_producer = new Producer(options.ToProducerProperties()); // todo: investigate other constructor options
			Name = providerName;
		}
		
		public async Task QueueMessageBatchAsync<T>(
			Guid streamGuid, 
			string streamNamespace, 
			IEnumerable<T> events, 
			StreamSequenceToken token,
			Dictionary<string, object> requestContext
		)
		{
			var queueId = _streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
//			var partitionId = (int)queueId.GetNumericId();

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
//					_queueProperties.Namespace, 
//					key,
//					0,
//					key.Length,
//					value,
//					0,
//					value.Length,
//					partitionId,
//					true
//				);
				
				// do we need orleans partition assigning since kafka already handels it for us?
				var message = await _producer.ProduceAsync(
					_queueProperties.Namespace, 
					streamGuid.ToByteArray(), 
					batch.ToByteArray(_serializationManager)
				);


				// todo: log message sent
			}
			catch (Exception ex)
			{
				// todo: log
			}
		}

		public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			_producer.Dispose(); // todo: is this enough?
		}
	}
}