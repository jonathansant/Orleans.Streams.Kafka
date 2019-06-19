using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.E2E.Grains;
using Orleans.Streams.Utils.MessageTracking;
using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class TestBase : IAsyncLifetime
	{
		private short _noOfSilos;
		protected TestCluster Cluster { get; private set; }

		//		public static string BrokerEndpoint = "kafka-dev-mw-0.rivertech.dev:19092";
		public static string BrokerEndpoint = "localhost:9092";

		protected void Initialize(short noOfSilos)
			=> _noOfSilos = noOfSilos;

		protected void ShutDown()
			=> Cluster?.StopAllSilos();

		public Task InitializeAsync()
		{
			var builder = new TestClusterBuilder(_noOfSilos);

			builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
			builder.AddClientBuilderConfigurator<ClientBuilderConfigurator>();

			Cluster = builder.Build();
			Cluster.Deploy();

			return Task.CompletedTask;
		}

		public Task DisposeAsync()
		{
			ShutDown();
			return Task.CompletedTask;
		}
	}

	public class ClientBuilderConfigurator : IClientBuilderConfigurator
	{
		public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
			=> clientBuilder
				.AddKafkaStreamProvider(Consts.KafkaStreamProvider, options =>
				{
					options.BrokerList = new List<string> { TestBase.BrokerEndpoint };
					options.ConsumerGroupId = "E2EGroup";

					options
						.AddTopic(Consts.StreamNamespace)
						.AddTopic(Consts.StreamNamespace2);

					options.PollTimeout = TimeSpan.FromMilliseconds(10);
					options.ConsumeMode = ConsumeMode.StreamEnd;
				})
				.ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(RoundTripGrain).Assembly).WithReferences())
		;
	}

	public class SiloBuilderConfigurator : ISiloBuilderConfigurator
	{
		public void Configure(ISiloHostBuilder hostBuilder)
			=> hostBuilder
				.AddMemoryGrainStorage("PubSubStore")
				.UseLoggingTracker()
				.AddKafkaStreamProvider(Consts.KafkaStreamProvider, options =>
				{
					options.BrokerList = new List<string> { TestBase.BrokerEndpoint };
					options.ConsumerGroupId = "E2EGroup";
					options.ConsumeMode = ConsumeMode.StreamEnd;
					options.PollTimeout = TimeSpan.FromMilliseconds(10);
					options.MessageTrackingEnabled = true;

					options
						.AddTopic(Consts.StreamNamespace)
						.AddTopic(Consts.StreamNamespace2);
				})
				.ConfigureApplicationParts(parts =>
					parts.AddApplicationPart(typeof(RoundTripGrain).Assembly).WithReferences())
				.UseLoggingTracker()
		;
	}
}
