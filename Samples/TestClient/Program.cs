using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Net;
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

			var clusterClient = await clientTask;

			var grainId = "PLAYER-5a98c80e-26b8-4d1c-a5da-cb64237f2392";
			var testGrain = clusterClient.GetGrain<ITestGrain>(grainId);

			var result = await testGrain.GetThePhrase();

			Console.BackgroundColor = ConsoleColor.DarkMagenta;
			Console.WriteLine(result);

			var streamProvider = clusterClient.GetStreamProvider("KafkaProvider");
			var stream = streamProvider.GetStream<TestModel>("streamId", "sucrose-test");

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

		private static Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 25)
		{
			var attempt = 0;
			IClusterClient client;
			while (true)
			{
				try
				{
					var siloAddress = IPAddress.Loopback;
					var gatewayPort = 30000;

					var brokers = new List<string>
					{
						"[host name]:39000",
						"[host name]:39001",
						"[host name]:39002"
					};
					var host = new HostBuilder().UseOrleansClient(builder => 
						builder	
						.Configure<ClusterOptions>(options =>
						{
							options.ClusterId = "TestCluster";
							options.ServiceId = "123";
						})
						.UseStaticClustering(options => options.Gateways.Add((new IPEndPoint(siloAddress, gatewayPort)).ToGatewayUri()))
						.AddKafka("KafkaProvider")
						.WithOptions(options =>
						{
							options.BrokerList = brokers;
							options.ConsumerGroupId = "TestGroup";
							options.AddTopic("sucrose-test");
						}))
						.Build();
					client = host.Services.GetRequiredService<IClusterClient>();

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

			return Task.FromResult(client);
		}
	}
}