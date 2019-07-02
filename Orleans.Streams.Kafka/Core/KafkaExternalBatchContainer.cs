using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[Serializable]
	public class KafkaExternalBatchContainer : KafkaBatchContainer
	{
		private readonly IExternalStreamDeserializer _deserializer;

		public KafkaExternalBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<byte[]> events,
			EventSequenceTokenV2 streamSequenceToken,
			TopicPartitionOffset offset,
			IExternalStreamDeserializer deserializer
		) : base(streamGuid, streamNamespace, events.Cast<object>().ToList(), null, streamSequenceToken, offset)
		{
			_deserializer = deserializer;
		}

		public override IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
			=> SerializeExternalEvents<T>((EventSequenceTokenV2)SequenceToken);

		private IEnumerable<Tuple<T, StreamSequenceToken>> SerializeExternalEvents<T>(EventSequenceTokenV2 sequenceToken)
		{
			var typedSerializer = _deserializer as IExternalStreamDeserializer<T>;

			var serializedEvents = Events
				.Select((@event, iteration) =>
				{
					try
					{
						T message;
						var messageType = typeof(T);

						if (messageType == typeof(byte[]))
							message = (T)@event;
						else if (messageType.IsPrimitive || messageType == typeof(string) || messageType == typeof(decimal))
							message = (T)Convert.ChangeType(@event, typeof(T));
						else
							message = typedSerializer.Deserialize((byte[])@event);

						return Tuple.Create<T, StreamSequenceToken>(message, sequenceToken.CreateSequenceTokenForEvent(iteration));
					}
					catch (Exception)
					{
						return null;
					}
				})
				.Where(@event => @event != null);

			return serializedEvents;
		}
	}
}