using Orleans.Streams.Utils.Streams;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IStreamRoundTripGrain : IBaseTestGrain
	{
	}

	public class StreamRoundTripGrain : BaseTestGrain, IStreamRoundTripGrain
	{
		private IStreamProvider _kafkaProvider;

		public override async Task OnActivateAsync()
		{
			_kafkaProvider = GetStreamProvider(Consts.KafkaStreamProvider);
			var testStream = _kafkaProvider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace);

			var subscriptionHandles = await testStream.GetAllSubscriptionHandles();
			if (subscriptionHandles.Count > 0)
			{
				foreach (var subscriptionHandle in subscriptionHandles)
				{
					await subscriptionHandle.ResumeAsync(OnNextTestMessage);
				}
			}

			await testStream.SubscribeAsync(OnNextTestMessage);
		}

		private Task OnNextTestMessage(TestModel message, StreamSequenceToken sequenceToken)
		{
			var responseStream = _kafkaProvider.GetStream<TestModel>(Consts.ResponseStreamId, Consts.StreamNamespace);
			return responseStream.OnNextAsync(message);
		}
	}
}
