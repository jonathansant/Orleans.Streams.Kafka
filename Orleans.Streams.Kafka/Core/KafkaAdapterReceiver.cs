using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Extensions;
using Orleans.Streams.Utils.Streams;
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
		private readonly QueueProperties _queueProperties;
		private readonly string _streamNamespace;

		private Consumer<byte[], byte[]> _consumer;
		private Task _outstandingPromise;

		public KafkaAdapterReceiver(
			QueueId queueId,
			QueueProperties queueProperties,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory
		)
		{
			_queueId = queueId ?? throw new ArgumentNullException(nameof(queueId));
			_queueProperties = queueProperties;
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_serializationManager = serializationManager;

			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();

			_streamNamespace = queueId.GetQueueNamespace();
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new Consumer<byte[], byte[]>(
				_options.ToConsumerProperties(),
				new ByteArrayDeserializer(),
				new ByteArrayDeserializer()
			);

			var offsetMode = Offset.Stored;
			switch (_options.ConsumeMode)
			{
				case ConsumeMode.LastCommittedMessage:
					offsetMode = Offset.Stored;
					break;
				case ConsumeMode.StreamEnd:
					offsetMode = Offset.End;
					break;
				case ConsumeMode.StreamStart:
					offsetMode = Offset.Beginning;
					break;
			}

			_consumer.Assign(new TopicPartitionOffset(_streamNamespace, (int)_queueProperties.PartitionId, offsetMode));

			return Task.CompletedTask;
		}

		public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return new List<IBatchContainer>();

			try
			{
				var messagePromise = _consumer.Poll(_options.PollTimeout);

				_outstandingPromise = messagePromise;

				var consumeResult = await messagePromise;
				if (consumeResult != null)
				{
					var batchContainer = consumeResult.ToBatchContainer(_serializationManager, _options, _streamNamespace);

					_logger.Info("Received batch: {@batch}", batchContainer);

					return new List<IBatchContainer> { batchContainer };
				}
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages queueId: {queueId}", _queueId);
				throw;
			}

			return new List<IBatchContainer>();
		}

		public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			try
			{
				if (!messages.Any())
					return Task.CompletedTask;

				var highestOffset = messages
					.Cast<KafkaBatchContainer>()
					.Max();

				_consumer.Commit(new[] { highestOffset.TopicPartitionOffSet });

				return Task.CompletedTask;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit messages. {@messages}", messages);
				throw;
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