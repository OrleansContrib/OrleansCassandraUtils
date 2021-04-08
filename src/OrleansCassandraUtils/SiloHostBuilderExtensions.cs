using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Messaging;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;
using OrleansCassandraUtils.Clustering;
using OrleansCassandraUtils.Persistence;
using OrleansCassandraUtils.Reminders;

namespace OrleansCassandraUtils
{
    public static class SiloHostBuilderExtensions
    {
        #region Clustering

        public static ISiloHostBuilder UseCassandraClustering(this ISiloHostBuilder builder, Action<CassandraClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                        services.Configure(configureOptions);

                    services.AddSingleton<IMembershipTable, CassandraClusteringTable>();
                });
        }

        public static ISiloHostBuilder UseCassandraClustering(this ISiloHostBuilder builder, Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<CassandraClusteringOptions>());
                    services.AddSingleton<IMembershipTable, CassandraClusteringTable>();
                });
        }

        public static ISiloBuilder UseCassandraClustering(this ISiloBuilder builder, Action<CassandraClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                        services.Configure(configureOptions);

                    services.AddSingleton<IMembershipTable, CassandraClusteringTable>();
                });
        }

        public static ISiloBuilder UseCassandraClustering(this ISiloBuilder builder, Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<CassandraClusteringOptions>());
                    services.AddSingleton<IMembershipTable, CassandraClusteringTable>();
                });
        }

        public static IClientBuilder UseCassandraClustering(this IClientBuilder builder, Action<CassandraClusteringOptions> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    if (configureOptions != null)
                        services.Configure(configureOptions);

                    services.AddSingleton<IGatewayListProvider, CassandraGatewayListProvider>();
                });
        }

        public static IClientBuilder UseCassandraClustering(this IClientBuilder builder, Action<OptionsBuilder<CassandraClusteringOptions>> configureOptions)
        {
            return builder.ConfigureServices(
                services =>
                {
                    configureOptions?.Invoke(services.AddOptions<CassandraClusteringOptions>());
                    services.AddSingleton<IGatewayListProvider, CassandraGatewayListProvider>();
                });
        }

        #endregion

        #region Reminders

        public static ISiloHostBuilder UseCassandraReminderService(this ISiloHostBuilder builder, Action<CassandraReminderTableOptions> configureOptions)
        {
            return builder.UseCassandraReminderService(ob => ob.Configure(configureOptions));
        }

        public static ISiloHostBuilder UseCassandraReminderService(this ISiloHostBuilder builder, Action<OptionsBuilder<CassandraReminderTableOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCassandraReminderService(configureOptions));
        }

        public static ISiloBuilder UseCassandraReminderService(this ISiloBuilder builder, Action<CassandraReminderTableOptions> configureOptions)
        {
            return builder.UseCassandraReminderService(ob => ob.Configure(configureOptions));
        }

        public static ISiloBuilder UseCassandraReminderService(this ISiloBuilder builder, Action<OptionsBuilder<CassandraReminderTableOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseCassandraReminderService(configureOptions));
        }

        public static IServiceCollection UseCassandraReminderService(this IServiceCollection services, Action<OptionsBuilder<CassandraReminderTableOptions>> configureOptions)
        {
            services.AddSingleton<IReminderTable, CassandraReminderTable>();
            services.ConfigureFormatter<CassandraReminderTableOptions>();
            configureOptions(services.AddOptions<CassandraReminderTableOptions>());
            return services;
        }

        #endregion

        #region Persistence

        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(this ISiloHostBuilder builder, Action<CassandraGrainStorageOptions> configureOptions)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloHostBuilder AddCassandraGrainStorage(this ISiloHostBuilder builder, string name, Action<CassandraGrainStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions));
        }

        public static ISiloHostBuilder AddCassandraGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<CassandraGrainStorageOptions>> configureOptions = null)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloHostBuilder AddCassandraGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<CassandraGrainStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions));
        }

        public static ISiloBuilder AddCassandraGrainStorageAsDefault(this ISiloBuilder builder, Action<CassandraGrainStorageOptions> configureOptions)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloBuilder AddCassandraGrainStorage(this ISiloBuilder builder, string name, Action<CassandraGrainStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions));
        }

        public static ISiloBuilder AddCassandraGrainStorageAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<CassandraGrainStorageOptions>> configureOptions = null)
        {
            return builder.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloBuilder AddCassandraGrainStorage(this ISiloBuilder builder, string name, Action<OptionsBuilder<CassandraGrainStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddCassandraGrainStorage(name, configureOptions));
        }

        public static IServiceCollection AddCassandraGrainStorage(this IServiceCollection services, Action<CassandraGrainStorageOptions> configureOptions)
        {
            return services.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection AddCassandraGrainStorage(this IServiceCollection services, string name, Action<CassandraGrainStorageOptions> configureOptions)
        {
            return services.AddCassandraGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection AddCassandraGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<CassandraGrainStorageOptions>> configureOptions = null)
        {
            return services.AddCassandraGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static IServiceCollection AddCassandraGrainStorage(this IServiceCollection services, string name, Action<OptionsBuilder<CassandraGrainStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<CassandraGrainStorageOptions>(name));
            services.ConfigureNamedOptionForLogging<CassandraGrainStorageOptions>(name);
            services.TryAddSingleton<IGrainStorage>(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            return services.AddSingletonNamedService<IGrainStorage>(name, CassandraGrainStorageFactory.Create)
                           .AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }

        #endregion
    }
}
