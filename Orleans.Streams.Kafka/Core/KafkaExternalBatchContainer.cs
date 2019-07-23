using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[Serializable]
	public class KafkaExternalBatchContainer : KafkaBatchContainer
	{
		private readonly QueueProperties _queueProps;
		private readonly IExternalStreamDeserializer _deserializer;

		public KafkaExternalBatchContainer(
			Guid streamGuid,
			QueueProperties queueProps,
			List<byte[]> events,
			EventSequenceTokenV2 streamSequenceToken,
			TopicPartitionOffset offset,
			IExternalStreamDeserializer deserializer
		) : base(streamGuid, queueProps.Namespace, events.Cast<object>().ToList(), null, streamSequenceToken, offset)
		{
			_queueProps = queueProps;
			_deserializer = deserializer;
		}

		public override IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			var sequenceToken = (EventSequenceTokenV2)SequenceToken;
			var serializedEvents = Events
				.Select((@event, iteration) => DeserializeExternalEvent<T>(sequenceToken, @event, iteration))
				.Where(@event => @event != null)
			;

			return serializedEvents;
		}

		private Tuple<T, StreamSequenceToken> DeserializeExternalEvent<T>(
			EventSequenceTokenV2 sequenceToken,
			object @event,
			int iteration
		)
		{
			T message;
			var messageType = typeof(T);

			if (messageType == typeof(byte[]))
				message = (T)@event;
			else if (messageType.IsPrimitive || messageType == typeof(string) || messageType == typeof(decimal))
				message = (T)Convert.ChangeType(@event, typeof(T));
			else
				message = _deserializer.Deserialize<T>(_queueProps, (byte[])@event);

			return Tuple.Create<T, StreamSequenceToken>(message, sequenceToken.CreateSequenceTokenForEvent(iteration));
		}
	}
}