using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Orleans.Hosting;
using Orleans.Streams.Kafka.E2E.Grains;
using Orleans.TestingHost;

namespace Orleans.Streams.Kafka.E2E.Tests
{
	public class TestBase : IDisposable
	{
		protected TestCluster Cluster { get; private set; }

		protected void Initialize(short noOfSilos)
		{
			var builder = new TestClusterBuilder(noOfSilos);

			builder.AddSiloBuilderConfigurator<SiloBuilderConfigurator>();
			builder.AddClientBuilderConfigurator<ClientBuilderConfigurator>();

			Cluster = builder.Build();
			Cluster.Deploy();
		}

		protected void ShutDown()
			=> Cluster?.StopAllSilos();

		public void Dispose()
			=> ShutDown();
	}

	public class ClientBuilderConfigurator : IClientBuilderConfigurator
	{
		public virtual void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
			=> clientBuilder
				.AddKafkaStreamProvider(Consts.KafkaStreamProvider, options =>
				{
					options.BrokerList = new List<string> { "localhost:9092" };
					options.ConsumerGroupId = "TestGroup";
					options.Topics = new List<string> { Consts.StreamNamespace };
				})
				.ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(StreamGrain).Assembly).WithReferences());
	}

	public class SiloBuilderConfigurator : ISiloBuilderConfigurator
	{
		public void Configure(ISiloHostBuilder hostBuilder)
			=> hostBuilder
				.AddMemoryGrainStorage("PubSubStore")
				.AddKafkaStreamProvider(Consts.KafkaStreamProvider, options =>
				{
					options.BrokerList = new List<string> { "localhost:9092" };
					options.ConsumerGroupId = "TestGroup";
					options.Topics = new List<string> { Consts.StreamNamespace };
				})
				.ConfigureApplicationParts(parts =>
					parts.AddApplicationPart(typeof(StreamGrain).Assembly).WithReferences());
	}
}
