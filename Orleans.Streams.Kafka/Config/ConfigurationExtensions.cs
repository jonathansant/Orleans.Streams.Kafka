using Confluent.SchemaRegistry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Kafka.Serialization;
using Orleans.Streams.Utils.Serialization;
using System;

// ReSharper disable once CheckNamespace
namespace Orleans.Hosting
{
	public static class ConfigurationExtensions
	{
		private const int DefaultCacheSize = 4096;

		public static KafkaStreamClientBuilder AddKafka(
			this IClientBuilder builder,
			string providerName
		)
			=> new KafkaStreamClientBuilder(builder, providerName);

		public static KafkaStreamSiloHostBuilder AddKafka(
			this ISiloBuilder builder,
			string providerName
		)
			=> new KafkaStreamSiloHostBuilder(builder, providerName);

		public static IClientBuilder AddKafkaStreamProvider(
			this IClientBuilder builder,
			string providerName,
			Action<KafkaStreamOptions> configureOptions
		)
			=> AddClientProvider(builder, providerName, opt => opt.Configure(configureOptions));

		private static IClientBuilder AddClientProvider(
			IClientBuilder builder,
			string providerName,
			Action<OptionsBuilder<KafkaStreamOptions>> configureOptions = null
		)
		{
			builder
				.ConfigureServices(services =>
				{
					services
						.ConfigureNamedOptionForLogging<KafkaStreamOptions>(providerName)
						.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(providerName)
						.AddJson(providerName)
					;
				})
				.AddPersistentStreams(providerName, KafkaAdapterFactory.Create, stream => stream.Configure(configureOptions))
				.Configure<SimpleQueueCacheOptions>(ob => ob.CacheSize = DefaultCacheSize)
				;

			return builder;
		}

		public static ISiloBuilder AddKafkaStreamProvider(
			this ISiloBuilder builder,
			string providerName,
			Action<KafkaStreamOptions> configureOptions
		) => AddSiloProvider(builder, providerName, opt => opt.Configure(configureOptions));

		private static ISiloBuilder AddSiloProvider(
			this ISiloBuilder builder,
			string providerName,
			Action<OptionsBuilder<KafkaStreamOptions>> configureOptions = null
		)
		{
			builder
				.ConfigureServices(services =>
				{
					services
						.ConfigureNamedOptionForLogging<KafkaStreamOptions>(providerName)
						.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(providerName)
					;
				})
				.AddPersistentStreams(providerName, KafkaAdapterFactory.Create,
					stream => stream.Configure(configureOptions))
				.Configure<SimpleQueueCacheOptions>(options => options.CacheSize = DefaultCacheSize);

			return builder;
		}

		public static ISiloBuilder AddAvro(
			this ISiloBuilder builder,
			string providerName,
			string registryUrl
		) => builder.ConfigureServices(services => services.AddAvro(providerName, registryUrl));

		public static IClientBuilder AddAvro(
			this IClientBuilder builder,
			string providerName,
			string registryUrl
		) => builder.ConfigureServices(services => services.AddAvro(providerName, registryUrl));
		
		public static ISiloBuilder AddJson(
			this ISiloBuilder builder,
			string providerName
		) => builder.ConfigureServices(services => services.AddJson(providerName));

		public static IClientBuilder AddJson(
			this IClientBuilder builder,
			string providerName
		) => builder.ConfigureServices(services => services.AddJson(providerName));

		private static void AddAvro(this IServiceCollection services, string providerName, string registryUrl)
			=> services
				.AddSingletonNamedService<ISchemaRegistryClient>(
					providerName,
					(provider, _) => ActivatorUtilities.CreateInstance<CachedSchemaRegistryClient>(
						provider,
						new SchemaRegistryConfig
						{
								Url = registryUrl
						})
				)
				.AddSingletonNamedService<IExternalStreamDeserializer>(
					providerName,
					(provider, _)
						=> ActivatorUtilities.CreateInstance<AvroExternalStreamDeserializer>(
							provider,
							provider.GetRequiredServiceByName<ISchemaRegistryClient>(providerName))
						);

		private static void AddJson(this IServiceCollection services, string providerName)
			=> services
				.AddSingletonNamedService<IExternalStreamDeserializer, JsonExternalStreamDeserializer>(providerName);
	}
}