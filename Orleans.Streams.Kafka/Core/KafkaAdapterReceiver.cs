using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly SerializationManager _serializationManager;
		private readonly QueueId _queueId;
		private readonly KafkaStreamOptions _options;
		private readonly IList<IBatchContainer> _messages;
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private TaskCompletionSource<bool> _currentBatchCompletion;
		private Task _outstandingTask;
		private Consumer _consumer;

		public KafkaAdapterReceiver(
			QueueId queueId,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_queueId = queueId ?? throw new ArgumentNullException("queueId");
			_options = options ?? throw new ArgumentNullException("options");
			
			_serializationManager = serializationManager;
			_messages = new List<IBatchContainer>();
			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
		}
		
		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new Consumer(_options.ToConsumerProperties());

			_consumer.OnMessage += OnMessageReceived;
			_consumer.OnError += (_, message) => _logger.LogError($"Error dequeueuing message due to: {message.Reason}");
			
			_consumer.Assign(new []{ new TopicPartitionOffset(
				_queueId.GetStringNamePrefix(), 
				(int)_queueId.GetNumericId(), 
				Offset.Stored)
			});
			
			return Task.CompletedTask;
		}

		public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			_messages.Clear();
			
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
			if (consumerRef == null)
				return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());
			
			_currentBatchCompletion = new TaskCompletionSource<bool>();
			_outstandingTask = _currentBatchCompletion.Task;
			
			_consumer.Poll(TimeSpan.FromMilliseconds(_options.PollTimeout));
			
			_currentBatchCompletion.SetResult(true);

			return Task.FromResult(_messages);
		}

		public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			throw new NotImplementedException();
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				if(_outstandingTask != null)
					await _outstandingTask;
			}
			finally
			{
				_consumer.Dispose();
				_consumer = null;
			}
		}

		private void OnMessageReceived(object sender, Message message)
		{
			var batch = KafkaBatchContainer.ToBatchContainer(message, _serializationManager);
			
			_logger.Info("Recieved batch: @batch", batch);
			_messages.Add(batch);
		}
	}
}