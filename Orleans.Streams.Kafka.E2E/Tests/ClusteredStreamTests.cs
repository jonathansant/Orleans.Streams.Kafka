using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Orleans.Streams.Kafka.E2E.Grains;
using Orleans.Streams.Utils.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class ClusteredStreamTests : TestBase
	{
		private const int ReceiveDelay = 500;

		public ClusteredStreamTests()
		{
			Initialize(3); // Initialize with three silos so that queues will be load-balanced on different silos.
		}

		[Fact]
		public async Task ProduceConsumeTest()
		{
			var grain = await WakeUpGrain<IStreamGrain>();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);

			var testMessage = TestModel.Random();

			await stream.OnNextAsync(testMessage);

			await Task.Delay(ReceiveDelay);
			var response = await grain.WhatDidIGet();

			Assert.Equal(testMessage, response);
		}


		[Fact]
		public async Task RoundTripTest()
		{
			await WakeUpGrain<IStreamRoundTripGrain>();
			var grain = await WakeUpGrain<IStreamGrain>();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId2, Consts.StreamNamespace);

			var testMessage = TestModel.Random();

			await stream.OnNextAsync(testMessage);

			await Task.Delay(ReceiveDelay);
			var response = await grain.WhatDidIGet();

			Assert.Equal(testMessage, response);
		}

		[Fact]
		public async Task ProduceConsumeExternalMessage()
		{
			var grain = await WakeUpGrain<IStreamGrain>();

			var config = GetKafkaServerConfig();

			var testMessage = TestModel.Random();

			using (var producer = new Producer<byte[], string>(
					config,
					new ByteArraySerializer(),
					new StringSerializer(Encoding.UTF8))
			)
			{
				await producer.ProduceAsync(Consts.StreamNamespace, new Message<byte[], string>
				{
					Key = Encoding.UTF8.GetBytes(Consts.StreamId),
					Value = JsonConvert.SerializeObject(testMessage),
					Headers = new Headers { new Header("external", BitConverter.GetBytes(true)) },
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				});
			}

			await Task.Delay(ReceiveDelay);
			var response = await grain.WhatDidIGet();

			Assert.Equal(testMessage, response);
		}

		[Fact]
		public async Task ProduceConsumeExternalMessageWithStringKey()
		{
			var grain = await WakeUpGrain<IStreamGrain>();

			var config = GetKafkaServerConfig();

			var testMessage = TestModel.Random();

			using (var producer = new Producer<string, string>(
				config,
				new StringSerializer(Encoding.UTF8),
				new StringSerializer(Encoding.UTF8))
			)
			{
				await producer.ProduceAsync(Consts.StreamNamespace, new Message<string, string>
				{
					Key = Consts.StreamId,
					Value = JsonConvert.SerializeObject(testMessage),
					Headers = new Headers { new Header("external", BitConverter.GetBytes(true)) },
					Timestamp = new Timestamp(DateTimeOffset.UtcNow)
				});
			}

			await Task.Delay(ReceiveDelay);
			var response = await grain.WhatDidIGet();

			Assert.Equal(testMessage, response);
		}

		[Fact]
		public async Task ConsumeInOrder()
		{
			var grain = await WakeUpGrain<IStreamGrain>();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);

			TestModel lastMessage = null;
			for (var i = 0; i < 10; i++)
			{
				var testMessage = TestModel.Random();
				await stream.OnNextAsync(testMessage);
				lastMessage = testMessage;
			}

			await Task.Delay(ReceiveDelay);
			var response = await grain.WhatDidIGet();

			Assert.Equal(lastMessage, response);
		}

		[Fact]
		public async Task ConsumeInOrderMultipleStreams()
		{
			var grain = await WakeUpGrain<IStreamGrain>();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);
			var stream2 = streamProvider.GetStream<TestModel>(Consts.StreamId3, Consts.StreamNamespace);

			TestModel lastMessage = null;
			TestModel lastMessage2 = null;
			for (var i = 0; i < 10; i++)
			{
				var testMessage = TestModel.Random();
				var testMessage2 = TestModel.Random();
				await Task.WhenAll(stream.OnNextAsync(testMessage), stream2.OnNextAsync(testMessage2));
				lastMessage = testMessage;
				lastMessage2 = testMessage2;
			}

			await Task.Delay(ReceiveDelay);
			var response = await grain.WhatDidIGet();
			var response2 = await grain.WhatDidIGet2();

			Assert.Equal(lastMessage, response);
			Assert.Equal(lastMessage2, response2);
		}

		[Fact]
		public async Task ConsumeInOrderMultipleStreamsWithDifferentNamespaces()
		{
			var grain = await WakeUpGrain<IStreamGrain>();
			var customGrain = Cluster.Client.GetGrain<ICustomizableStreamGrain>($"{Consts.StreamNamespace2}-{Consts.StreamId}");
			await customGrain.WakeUp();

			var streamProvider = Cluster.Client.GetStreamProvider(Consts.KafkaStreamProvider);
			var stream = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace);
			var stream2 = streamProvider.GetStream<TestModel>(Consts.StreamId, Consts.StreamNamespace2);

			TestModel lastMessage = null;
			TestModel lastMessage2 = null;
			for (var i = 0; i < 10; i++)
			{
				var testMessage = TestModel.Random();
				var testMessage2 = TestModel.Random();
				await Task.WhenAll(stream.OnNextAsync(testMessage), stream2.OnNextAsync(testMessage2));
				lastMessage = testMessage;
				lastMessage2 = testMessage2;
			}

			await Task.Delay(ReceiveDelay + 4000);
			var response = await grain.WhatDidIGet();
			var response2 = await customGrain.WhatDidIGet();

			Assert.Equal(lastMessage, response);
			Assert.Equal(lastMessage2, response2);
		}

		private async Task<TGrain> WakeUpGrain<TGrain>() where TGrain : IBaseTestGrain
		{
			var grain = Cluster.GrainFactory.GetGrain<TGrain>("testGrain");
			await grain.WakeUp();

			return grain;
		}

		private static IDictionary<string, object> GetKafkaServerConfig()
			=> new Dictionary<string, object>
			{
				{"bootstrap.servers", "pkc-l9pve.eu-west-1.aws.confluent.cloud:9092"},
				{"api.version.request", true},
				{"broker.version.fallback", "0.10.0.0"},
				{"api.version.fallback.ms", 0},
				{"sasl.mechanisms", "PLAIN"},
				{"security.protocol", "SASL_SSL"},
				{"ssl.ca.location", Environment.GetEnvironmentVariable("sslCaLocation")},
				{"sasl.username", Environment.GetEnvironmentVariable("userName")},
				{"sasl.password", Environment.GetEnvironmentVariable("password")},
			};
	}
}
