using Confluent.Kafka;
using Newtonsoft.Json;
using Orleans.Streams.Kafka.E2E.Extensions;
using Orleans.Streams.Kafka.E2E.Grains;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class ClusteredStreamTests : TestBase
	{
		private const int ReceiveDelay = 1000;

		public ClusteredStreamTests()
		{
			Initialize(3); // Initialize with three silos so that queues will be load-balanced on different silos.
		}

		[Fact]
		public async Task ProduceConsumeTest()
		{
			var grain = await WakeUpGrain<IStreamGrainV2>();
			var result = await grain.Fire();

			Assert.Equal(result.Expected, result.Actual);
		}

		[Fact]
		public async Task ProduceConsumeExternalMessage()
		{
			var config = GetKafkaServerConfig();

			var testMessage = TestModel.Random();

			var completion = new TaskCompletionSource<bool>();

			var provider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = provider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace2);

			await stream.QuickSubscribe((message, seq) =>
			{
				Assert.Equal(testMessage, message);
				completion.SetResult(true);
				return Task.CompletedTask;
			});

			using (var producer = new ProducerBuilder<byte[], string>(config).Build())
			{
				await producer.ProduceAsync(Consts.StreamNamespace2, new Message<byte[], string>
				{
					Key = Encoding.UTF8.GetBytes(Consts.StreamId2),
					Value = JsonConvert.SerializeObject(testMessage),
					Headers = new Headers { new Header("x-external-message", BitConverter.GetBytes(true)) },
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				});
			}

			await Task.WhenAny(completion.Task, Task.Delay(ReceiveDelay * 4));

			if (!completion.Task.IsCompleted)
				throw new XunitException("Message not received.");
		}

		[Fact]
		public async Task ConsumeInOrderMultipleStreams()
		{
			var grain = await WakeUpGrain<IMultiStreamGrain>();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);
			var stream2 = streamProvider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace2);

			var result = grain.Fire();

			TestModel lastMessage = null;
			TestModel lastMessage2 = null;
			for (var i = 0; i < 10; i++)
			{
				var testMessage = TestModel.Random();
				var testMessage2 = TestModel.Random();

				if (i == 9)
				{
					testMessage.IsLastMessage = true;
					testMessage2.IsLastMessage = true;
				}

				await Task.WhenAll(stream.OnNextAsync(testMessage), stream2.OnNextAsync(testMessage2));
				lastMessage = testMessage;
				lastMessage2 = testMessage2;
			}

			await Task.WhenAny(result, Task.Delay(ReceiveDelay * 6));

			if (!result.IsCompleted)
				throw new XunitException("Message not received.");

			Assert.Equal(lastMessage, result.Result.StreamResult1.Actual);
			Assert.Equal(lastMessage2, result.Result.StreamResult2.Actual);
		}

		private async Task<TGrain> WakeUpGrain<TGrain>() where TGrain : IBaseTestGrain
		{
			var grain = Cluster.GrainFactory.GetGrain<TGrain>("testGrain");
			await grain.WakeUp();

			return grain;
		}

		private static IDictionary<string, string> GetKafkaServerConfig()
			=> new Dictionary<string, string>
			{
				{"bootstrap.servers", "localhost:9092"},
				{"api.version.request", "true"},
				{"broker.version.fallback", "0.10.0.0"},
				{"api.version.fallback.ms", 0.ToString()}
			};
	}
}