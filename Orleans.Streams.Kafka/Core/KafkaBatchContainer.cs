using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[GenerateSerializer]
	public class KafkaBatchContainer : IBatchContainer, IComparable<KafkaBatchContainer>
	{
		private readonly Dictionary<string, object> _requestContext;

		internal TopicPartitionOffset TopicPartitionOffSet;

		[Id(0)]
		public List<object> Events { get; set; }

		[Id(1)]
		public StreamId StreamId { get; set; }

		[Id(2)]
		public StreamSequenceToken SequenceToken { get; internal set; }

		public KafkaBatchContainer()
		{
		}

		public KafkaBatchContainer(
			StreamId streamId,
			List<object> events,
			Dictionary<string, object> requestContext
		) : this(streamId, events, requestContext, null, null)
		{
		}

		public KafkaBatchContainer(
			StreamId streamId,
			List<object> events,
			Dictionary<string, object> requestContext,
			EventSequenceTokenV2 streamSequenceToken,
			TopicPartitionOffset offset
		)
		{
			Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events.");

			StreamId = streamId;
			SequenceToken = streamSequenceToken;
			TopicPartitionOffSet = offset;
			_requestContext = requestContext;
		}

		public virtual IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			var sequenceToken = (EventSequenceTokenV2)SequenceToken;
			return Events
				.OfType<T>()
				.Select((@event, iteration) =>
					Tuple.Create<T, StreamSequenceToken>(
						@event,
						sequenceToken.CreateSequenceTokenForEvent(iteration))
				);
		}

		public bool ImportRequestContext()
		{
			if (_requestContext == null)
				return false;

			foreach (var contextProperties in _requestContext)
				Runtime.RequestContext.Set(contextProperties.Key, contextProperties.Value);

			return true;
		}

		public int CompareTo(KafkaBatchContainer other)
			=> TopicPartitionOffSet.Offset.Value.CompareTo(other.TopicPartitionOffSet.Offset.Value);

		public override string ToString()
			=> $"[{GetType().Name}:Stream={StreamId.GetNamespace()}.{StreamId.GetKeyAsString()},#Items={Events.Count}]";
	}
}