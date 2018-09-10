using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TestGrains;

namespace TestClient
{
	internal class Program
	{
		private static async Task Main(string[] args)
		{
			Console.Title = "Client";

			var clientTask = StartClientWithRetries();
			clientTask.Wait();

			var clusterClient = clientTask.Result;

			var testGrain = clusterClient.GetGrain<ITestGrain>("PLAYER-5a98c80e-26b8-4d1c-a5da-cb64237f2392");

			var result = await testGrain.GetThePhrase();

			Console.BackgroundColor = ConsoleColor.DarkMagenta;
			Console.WriteLine(result);

			var streamId = testGrain.GetPrimaryKeyString();

			var streamProvider = clientTask.Result.GetStreamProvider("KafkaProvider");
			var stream = streamProvider.GetStream<TestModel>(streamId, "gossip-testing");

			string line;
			while ((line = Console.ReadLine()) != string.Empty)
			{
				await stream.OnNextAsync(new TestModel
				{
					Greeting = line
				});
			}
			Console.ReadKey();
		}

		private static async Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 7)
		{
			var attempt = 0;
			IClusterClient client;
			while (true)
			{
				try
				{
					var siloAddress = IPAddress.Loopback;
					var gatewayPort = 30000;

					client = new ClientBuilder()
						.Configure<ClusterOptions>(options =>
						{
							options.ClusterId = "TestCluster";
							options.ServiceId = "123";
						})
						.UseStaticClustering(options => options.Gateways.Add((new IPEndPoint(siloAddress, gatewayPort)).ToGatewayUri()))
						.ConfigureApplicationParts(parts => parts.AddApplicationPart(Assembly.Load("TestGrains")).WithReferences())
						.ConfigureLogging(logging => logging.AddConsole())
						.AddKafkaStreamProvider("KafkaProvider", options =>
						{
							options.BrokerList = new List<string> { "localhost:9092" };
							options.ConsumerGroupId = "TestGroup";
							options.ExternalMessageIdentifier = "external";
							options.Topics = new List<string> { "gossip-testing" };
						})
						.Build();

					await client.Connect();

					Console.WriteLine("Client successfully connect to silo host");
					break;
				}
				catch (SiloUnavailableException)
				{
					attempt++;
					Console.WriteLine(
						$"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
					if (attempt > initializeAttemptsBeforeFailing)
					{
						throw;
					}
					Thread.Sleep(TimeSpan.FromSeconds(3));
				}
			}

			return client;
		}
	}
}