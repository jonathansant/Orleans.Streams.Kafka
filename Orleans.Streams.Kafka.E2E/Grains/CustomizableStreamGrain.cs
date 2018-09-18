using System.Threading.Tasks;
using Orleans.Streams.Kafka.E2E.Extensions;
using Orleans.Streams.Utils.Streams;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public interface ICustomizableStreamGrain : IBaseTestGrain
	{
		Task<TestModel> WhatDidIGet();
	}

	public class CustomizableStreamGrain : BaseTestGrain, ICustomizableStreamGrain
	{
		private TestModel _state;

		public Task<TestModel> WhatDidIGet() 
			=> Task.FromResult(_state);

		public override async Task OnActivateAsync()
		{
			var kafkaProvider = GetStreamProvider(Consts.KafkaStreamProvider);

			var primaryKeyParts = this.GetPrimaryKeyString().Split('-');

			var testStream = kafkaProvider.GetStream<TestModel>(primaryKeyParts[1], primaryKeyParts[0]);
			await testStream.QuickSubscribe(OnNextTestMessage);
		}

		private Task OnNextTestMessage(TestModel message, StreamSequenceToken sequenceToken)
		{
			_state = message;
			return Task.CompletedTask;
		}
	}
}
