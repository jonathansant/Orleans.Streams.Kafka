using System.Threading.Tasks;
using Orleans.Streams.Kafka.E2E.Extensions;
using Orleans.Streams.Utils.Streams;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public class StreamGrain : BaseTestGrain, IStreamGrain
	{
		private TestModel _state;
		private TestModel _state2;

		public Task<string> SaySomething(string something) 
			=> Task.FromResult(something);

		public Task<TestModel> WhatDidIGet() 
			=> Task.FromResult(_state);

		public Task<TestModel> WhatDidIGet2()
			=> Task.FromResult(_state2);

		public override async Task OnActivateAsync()
		{
			var kafkaProvider = GetStreamProvider(Consts.KafkaStreamProvider);
			var testStream = kafkaProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);
			var testStream2 = kafkaProvider.GetStream<TestModel>(Consts.StreamId3, Consts.StreamNamespace);

			await Task.WhenAll(testStream.QuickSubscribe(OnNextTestMessage), testStream2.QuickSubscribe(OnNextTestMessage2));
		}

		private Task OnNextTestMessage(TestModel message, StreamSequenceToken sequenceToken)
		{
			_state = message;
			return Task.CompletedTask;
		}

		private Task OnNextTestMessage2(TestModel message, StreamSequenceToken sequenceToken)
		{
			_state2 = message;
			return Task.CompletedTask;
		}
	}
}
