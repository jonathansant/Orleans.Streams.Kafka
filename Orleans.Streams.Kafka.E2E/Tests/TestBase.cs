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

		public static string BrokerEndpoint = "dev-data.rivertech.dev:39000";
		//		public static string BrokerEndpoint = "localhost:9092";

		public static List<string> Brokers = new List<string>
		{
			BrokerEndpoint,
			"dev-data.rivertech.dev:39001",
			"dev-data.rivertech.dev:39002"
		};

		private TestClusterBuilder _builder;

		protected void Initialize(short noOfSilos)
			=> Initialize<ClientBuilderConfigurator, SiloBuilderConfigurator>(noOfSilos);

		protected void Initialize<TClientConfig, TSiloConfig>(short noOfSilos)
			where TSiloConfig : ISiloBuilderConfigurator, new()
			where TClientConfig : IClientBuilderConfigurator, new()
		{
			_noOfSilos = noOfSilos;
			_builder = new TestClusterBuilder(_noOfSilos);
			_builder.AddSiloBuilderConfigurator<TSiloConfig>();
			_builder.AddClientBuilderConfigurator<TClientConfig>();
		}

		protected void ShutDown()
			=> Cluster?.StopAllSilos();

		public Task InitializeAsync()
		{
			Cluster = _builder.Build();
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
					options.BrokerList = TestBase.Brokers;
					options.ConsumerGroupId = "E2EGroup_client";

					options
						.AddTopic(Consts.StreamNamespace)
						.AddTopic(Consts.StreamNamespace2)
						.AddExternalTopic(Consts.StreamNamespaceExternal)
						;

					options.PollTimeout = TimeSpan.FromMilliseconds(10);
					options.ConsumeMode = ConsumeMode.StreamEnd;
				})
				.AddJson(Consts.KafkaStreamProvider)
				.ConfigureApplicationParts(parts =>
					parts.AddApplicationPart(typeof(RoundTripGrain).Assembly).WithReferences());
	}

	public class SiloBuilderConfigurator : ISiloBuilderConfigurator
	{
		public void Configure(ISiloHostBuilder hostBuilder)
			=> hostBuilder
				.AddMemoryGrainStorage("PubSubStore")
				.UseLoggingTracker()
				.AddKafkaStreamProvider(Consts.KafkaStreamProvider, options =>
				{
					options.BrokerList = TestBase.Brokers;
					options.ConsumerGroupId = "E2EGroup";
					options.ConsumeMode = ConsumeMode.StreamEnd;
					options.PollTimeout = TimeSpan.FromMilliseconds(10);
					options.MessageTrackingEnabled = true;

					options
						.AddTopic(Consts.StreamNamespace)
						.AddTopic(Consts.StreamNamespace2)
						.AddExternalTopic(Consts.StreamNamespaceExternal)
						;
				})
				.AddJson(Consts.KafkaStreamProvider)
				.ConfigureApplicationParts(parts =>
					parts.AddApplicationPart(typeof(RoundTripGrain).Assembly).WithReferences())
				.UseLoggingTracker();
	}
}
