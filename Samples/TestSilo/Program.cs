using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Streams.Utils.MessageTracking;

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
				.UseLoggingTracker()
				.AddKafkaStreamProvider("KafkaProvider", options =>
				{
					options.BrokerList = new List<string> { "localhost:9092" };
					options.ConsumerGroupId = "TestGroup";
					options.ExternalMessageIdentifier = "external";
					options.Topics = new List<string> { "gossip-testing" };
					options.MessageTrackingEnabled = true;
				});

			var host = builder.Build();
			await host.StartAsync();

			Console.ReadKey();

			await host.StopAsync();
		}
	}
}