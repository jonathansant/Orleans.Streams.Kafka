using Orleans.Streams.Utils.Streams;
using System.Threading.Tasks;
using Orleans.Streams.Kafka.E2E.Extensions;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface IStreamRoundTripGrain : IBaseTestGrain
	{
	}

	public class StreamRoundTripGrain : BaseTestGrain, IStreamRoundTripGrain
	{
		private IStreamProvider _kafkaProvider;
		private IAsyncStream<TestModel> _responseStream;

		public override async Task OnActivateAsync()
		{
			_kafkaProvider = GetStreamProvider(Consts.KafkaStreamProvider);
			var testStream = _kafkaProvider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace);
			_responseStream = _kafkaProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);

			await testStream.QuickSubscribe(OnNextTestMessage);
		}

		private async Task OnNextTestMessage(TestModel message, StreamSequenceToken sequenceToken) 
			=> await _responseStream.OnNextAsync(message);
	}
}
