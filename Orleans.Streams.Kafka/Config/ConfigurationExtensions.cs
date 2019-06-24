using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Core;
using System;

// ReSharper disable once CheckNamespace
namespace Orleans.Hosting
{
	public static class ConfigurationExtensions
	{
		private const int DefaultCacheSize = 4096;

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
				.ConfigureApplicationParts(parts => parts.AddFrameworkPart(typeof(KafkaAdapterFactory).Assembly).WithReferences())
				.ConfigureServices(services =>
				{
					services
						.ConfigureNamedOptionForLogging<KafkaStreamOptions>(providerName)
						.ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(providerName)
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
				.ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(KafkaAdapterFactory).Assembly).WithReferences())
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

		public static ISiloHostBuilder AddKafkaStreamProvider(
			this ISiloHostBuilder builder,
			string providerName,
			Action<KafkaStreamOptions> configureOptions
		) => AddSiloProvider(builder, providerName, opt => opt.Configure(configureOptions));

		private static ISiloHostBuilder AddSiloProvider(
			this ISiloHostBuilder builder,
			string providerName,
			Action<OptionsBuilder<KafkaStreamOptions>> configureOptions = null
		)
		{
			builder
				.ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(KafkaAdapterFactory).Assembly).WithReferences())
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
	}
}