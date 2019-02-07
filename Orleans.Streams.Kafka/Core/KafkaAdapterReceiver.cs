using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Consumer;
using Orleans.Streams.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly KafkaStreamOptions _options;
		private readonly SerializationManager _serializationManager;
		private readonly IGrainFactory _grainFactory;
		private readonly QueueProperties _queueProperties;

		private Consumer<byte[], byte[]> _consumer;
		private Task _commitPromise = Task.CompletedTask;

		public KafkaAdapterReceiver(
			QueueProperties queueProperties,
			KafkaStreamOptions options,
			SerializationManager serializationManager,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_queueProperties = queueProperties;
			_serializationManager = serializationManager;
			_grainFactory = grainFactory;
			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new Consumer<byte[], byte[]>(_options.ToConsumerProperties());

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

			_consumer.OnError += (sender, errorEvent) =>
				_logger.LogError(
					"Consume error reason: {reason}, code: {code}, is broker error: {errorType}",
					errorEvent.Reason,
					errorEvent.Code,
					errorEvent.IsBrokerError
				);

			_consumer.Assign(new TopicPartitionOffset(_queueProperties.Namespace, (int)_queueProperties.PartitionId, offsetMode));

			return Task.CompletedTask;
		}

		public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

			var cancellationSource = new CancellationTokenSource();
			cancellationSource.CancelAfter(_options.PollBufferTimeout);

			return Task.Run(
				() => PollForMessages(
					maxCount,
					cancellationSource
				),
				cancellationSource.Token
			);
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			KafkaBatchContainer batchWithHighestOffset = null;

			try
			{
				if (!messages.Any())
					return;

				batchWithHighestOffset = messages
					.Cast<KafkaBatchContainer>()
					.Max();

				var commitPromise = _consumer.Commit(new[] { batchWithHighestOffset });
				_commitPromise = commitPromise;

				await commitPromise;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to commit message offset: {@offset}", batchWithHighestOffset?.TopicPartitionOffSet);
				throw;
			}
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			try
			{
				await _commitPromise;
			}
			finally
			{
				_consumer.Unassign();
				_consumer.Unsubscribe();
				_consumer.Dispose();
				_consumer = null;
			}
		}

		private async Task<IList<IBatchContainer>> PollForMessages(int maxCount, CancellationTokenSource cancellation)
		{
			try
			{
				var batches = new List<IBatchContainer>();
				for (var i = 0; i < maxCount && !cancellation.IsCancellationRequested; i++)
				{
					var consumeResult = _consumer.Consume(_options.PollTimeout);
					if (consumeResult == null)
						break;

					var batchContainer = consumeResult.ToBatchContainer(
						_serializationManager,
						_options,
						_queueProperties.Namespace
					);

					await TrackMessage(batchContainer);

					batches.Add(batchContainer);
				}

				return batches;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages queueId: {@queueProperties}", _queueProperties);
				throw;
			}
			finally
			{
				cancellation.Dispose();
			}
		}

		private Task TrackMessage(IBatchContainer container)
		{
			if (!_options.MessageTrackingEnabled)
				return Task.CompletedTask;

			var trackingGrain = _grainFactory.GetMessageTrackerGrain(_queueProperties.QueueName);
			return trackingGrain.Track(container);
		}
	}
}