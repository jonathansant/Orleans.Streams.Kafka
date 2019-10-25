using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streams.Kafka.Config;
using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;

namespace TestSilo
{
	internal class Program
	{
		public static async Task Main(string[] args)
		{
			Console.Title = "Silo 1";

			const int siloPort = 11111;
			const int gatewayPort = 30000;
			var siloAddress = IPAddress.Loopback;

			var brokers = new List<string>
			{
				"[host name]:39000",
				"[host name]:39001",
				"[host name]:39002"
			};

			var builder = new SiloHostBuilder()
				.Configure<ClusterOptions>(options =>
				{
					//options.SiloName = "TestCluster";
					options.ClusterId = "TestCluster";
					options.ServiceId = "123";
				})
				.UseDevelopmentClustering(options => options.PrimarySiloEndpoint = new IPEndPoint(siloAddress, siloPort))
				.ConfigureEndpoints(siloAddress, siloPort, gatewayPort)
				.ConfigureApplicationParts(parts => parts.AddApplicationPart(Assembly.Load("TestGrains")).WithReferences())
				.ConfigureLogging(logging => logging.AddConsole())
				.AddMemoryGrainStorageAsDefault()
				.AddMemoryGrainStorage("PubSubStore")
				.AddKafka("KafkaProvider")
				.WithOptions(options =>
				{
					options.BrokerList = brokers.ToArray();
					options.ConsumerGroupId = "TestGroup";
					options.MessageTrackingEnabled = true;
					options.AddTopic("sucrose-test");
					options.AddTopic("sucrose-auto", new TopicCreationConfig { AutoCreate = true, Partitions = 2, ReplicationFactor = 1 });
					options.AddTopic("sucrose-auto2", new TopicCreationConfig { AutoCreate = true, Partitions = 3, ReplicationFactor = 1 });
				})
				.AddLoggingTracker()
				.Build();

			var host = builder.Build();
			await host.StartAsync();

			Console.ReadKey();

			await host.StopAsync();
		}
	}
}