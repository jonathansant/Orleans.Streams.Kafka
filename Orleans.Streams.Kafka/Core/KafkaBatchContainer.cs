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
		[Id(0)]
		private readonly Dictionary<string, object> _requestContext;

		[NonSerialized] internal TopicPartitionOffset TopicPartitionOffSet;

		[Id(1)]
		protected List<object> Events { get; set; }

		[Id(2)]
		public Guid StreamGuid { get; }

		[Id(3)]
		public string StreamNamespace { get; }

		[Id(4)]
		public StreamSequenceToken SequenceToken { get; internal set; }

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext
		) : this(streamGuid, streamNamespace, events, requestContext, null, null)
		{
		}

		public KafkaBatchContainer(
			Guid streamGuid,
			string streamNamespace,
			List<object> events,
			Dictionary<string, object> requestContext,
			EventSequenceTokenV2 streamSequenceToken,
			TopicPartitionOffset offset
		)
		{
			Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events.");

			StreamGuid = streamGuid;
			StreamNamespace = streamNamespace;
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
				RequestContext.Set(contextProperties.Key, contextProperties.Value);

			return true;
		}

		[Id(5)]
		public StreamId StreamId { get; }

		public int CompareTo(KafkaBatchContainer other)
			=> TopicPartitionOffSet.Offset.Value.CompareTo(other.TopicPartitionOffSet.Offset.Value);

		public override string ToString()
			=> $"[{GetType().Name}:Stream={StreamGuid},#Items={Events.Count}]";
	}
}