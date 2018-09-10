using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly QueueId _queueId;
		private readonly List<ConsumeResult<byte[], KafkaBatchContainer>> _consumedMessages;
		private Consumer<byte[], KafkaBatchContainer> _consumer;
		private Task _outstandingPromise;

		public KafkaAdapterReceiver(
			QueueId queueId,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_queueId = queueId ?? throw new ArgumentNullException(nameof(queueId));
			_options = options ?? throw new ArgumentNullException(nameof(options));
			_serializationManager = serializationManager;
			_consumedMessages = new List<ConsumeResult<byte[], KafkaBatchContainer>>();

			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new Consumer<byte[], KafkaBatchContainer>(
				_options.ToConsumerProperties(),
				new ByteArrayDeserializer(),
				new BatchContainerDeserializer(_serializationManager)
			);

			_consumer.Assign(new TopicPartitionOffset(_queueId.GetStringNamePrefix(), (int)_queueId.GetNumericId(), Offset.Beginning));
			return Task.CompletedTask;
		}

		public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return new List<IBatchContainer>();

			try
			{
				var messagePromise = _consumer.Poll(TimeSpan.FromMilliseconds(_options.PollTimeout));
				_outstandingPromise = messagePromise;

				var consumeResult = await messagePromise;
				_consumedMessages.Add(consumeResult);

				_logger.Info("Received batch: {@batch}", consumeResult.Value);

				return new List<IBatchContainer> { consumeResult.Value };
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages.");
				throw;
			}
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			try
			{
				var messagesCommitted = await _consumer.Commit(_consumedMessages);
				foreach (var topicPartitionOffset in messagesCommitted)
				{
					_consumedMessages.Remove(_consumedMessages.First(msg =>
						msg.TopicPartitionOffset == topicPartitionOffset)
					);
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit messages. {@messages}", messages);
				throw;// todo: should we throw?
			}
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				if (_outstandingPromise != null)
					await _outstandingPromise;
			}
			finally
			{
				_consumer.Dispose();
				_consumer = null;
			}
		}
	}
}