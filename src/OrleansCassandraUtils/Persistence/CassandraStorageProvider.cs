using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Cassandra;
using OrleansCassandraUtils.Utils;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using System.Threading;

namespace OrleansCassandraUtils.Persistence
{
    public static class CassandraGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<CassandraGrainStorageOptions> optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<CassandraGrainStorageOptions>>();
            return ActivatorUtilities.CreateInstance<CassandraGrainStorage>(services, Options.Create(optionsSnapshot.Get(name)), name);
        }
    }

    class CassandraGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        class TypeSerializationInfo
        {
            public CassandraGrainStorageOptions.SerializationProviderInfo providerInfo;
            public string typeString;
        }


        ILogger logger;
        CassandraGrainStorageOptions options;
        IProviderRuntime providerRuntime;
        string name;
        ISession session;
        OrleansQueries queries;

        ConcurrentDictionary<Type, TypeSerializationInfo> typeInfoCache = new ConcurrentDictionary<Type, TypeSerializationInfo>();


        public CassandraGrainStorage(ILogger<CassandraGrainStorage> logger, IProviderRuntime providerRuntime, IOptions<CassandraGrainStorageOptions> options, string name)
        {
            this.logger = logger;
            this.options = options.Value;
            this.providerRuntime = providerRuntime;
            this.name = name;
        }

        void ILifecycleParticipant<ISiloLifecycle>.Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<CassandraGrainStorage>(name), options.InitStage, Init);
        }

        async Task Init(CancellationToken cancellationToken)
        {
            if (!string.IsNullOrEmpty(options.ConnctionString))
                throw new BadProviderConfigException($"Connection string not specified for cassandra grain storage '{name}'.");

            foreach (var providerInfo in options.SerializationProviders.Values)
            {
                providerInfo.provider.Init(providerRuntime);
            }

            if (options.SerializationProviders.Count == 1)
                logger.Warn(0, $"No serialization providers have been specified, will use default serialzer " +
                    $"on top of Orleans serialization. This serializer does NOT support versioning. Specify " +
                    $"custom providers using `CassandraGrainStorageOptions.AddSerializationProvider`.");

            session = await CassandraSessionFactory.CreateSession(options.ConnctionString);
            queries = await OrleansQueries.CreateInstance(session);
        }

        byte[] GetKeyAsBlob(GrainReference GrainReference)
        {
            byte[] result;

            if (GrainReference.IsPrimaryKeyBasedOnLong())
            {
                long PK = GrainReference.GetPrimaryKeyLong(out string ext);
                if (PK == 0)
                {
                    if (ext == null)
                        result = new byte[] { 0 };
                    else
                    {
                        result = new byte[1 + Encoding.UTF8.GetByteCount(ext)];
                        result[0] = 1;
                        Encoding.UTF8.GetBytes(ext, 0, ext.Length, result, 1);
                    }
                }
                else
                {
                    if (ext == null)
                    {
                        result = new byte[9];
                        result[0] = 2;
                        Array.Copy(BitConverter.GetBytes(PK), 0, result, 1, 8);
                    }
                    else
                    {
                        result = new byte[9 + Encoding.UTF8.GetByteCount(ext)];
                        result[0] = 3;
                        Array.Copy(BitConverter.GetBytes(PK), 0, result, 1, 8);
                        Encoding.UTF8.GetBytes(ext, 0, ext.Length, result, 9);
                    }
                }
            }
            else
            {
                Guid PK = GrainReference.GetPrimaryKey(out string ext);
                if (ext == null)
                {
                    result = new byte[17];
                    result[0] = 4;
                    Array.Copy(PK.ToByteArray(), 0, result, 1, 16);
                }
                else
                {
                    result = new byte[17 + Encoding.UTF8.GetByteCount(ext)];
                    result[0] = 5;
                    Array.Copy(PK.ToByteArray(), 0, result, 1, 16);
                    Encoding.UTF8.GetBytes(ext, 0, ext.Length, result, 17);
                }
            }

            return result;
        }

        TypeSerializationInfo GetTypeSerializationInfo(string OrleansTypeString, Type Type)
        {
            if (!typeInfoCache.TryGetValue(Type, out var typeInfo))
            {
                foreach (var providerInfo in options.SerializationProviders.Values)
                {
                    if (providerInfo.provider.IsSupportedType(Type))
                    {
                        typeInfo = new TypeSerializationInfo()
                        {
                            typeString = providerInfo.provider.GetTypeString(OrleansTypeString, Type),
                            providerInfo = providerInfo
                        };
                        if (typeInfo.typeString.Contains("!"))
                            throw new ArgumentException($"TypeString returned from storage serialization providers may not contain the special character '!', got {typeInfo.typeString}");
                        break;
                    }
                }

                typeInfoCache.TryAdd(Type, typeInfo);
            }

            return typeInfo;
        }

        Task IGrainStorage.ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (string.IsNullOrEmpty(grainState.ETag))
                return Task.CompletedTask; // If the grain has no ETag, it has never been persisted to database

            var grainStateType = grainState.State.GetType();
            var typeInfo = GetTypeSerializationInfo(grainType, grainStateType);
            var keyBlob = GetKeyAsBlob(grainReference);

            return session.ExecuteAsync(queries.ClearStorage(typeInfo.typeString, keyBlob, Guid.Parse(grainState.ETag)));
        }

        async Task IGrainStorage.ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainStateType = grainState.State.GetType();
            var typeInfo = GetTypeSerializationInfo(grainType, grainStateType);
            var keyBlob = GetKeyAsBlob(grainReference);

            var rows = await session.ExecuteAsync(queries.ReadFromStorage(typeInfo.typeString, keyBlob));

            var row = rows.FirstOrDefault();

            if (row == null)
            {
                grainState.State = Activator.CreateInstance(grainStateType);
                grainState.ETag = null;
                return;
            }

            var data = (byte[])row["data"];
            var eTag = (Guid?)row["etag"];
            var serializerCode = (sbyte?)row["serializer_code"];
            if (data == null || eTag == null || serializerCode == null)
                throw new InconsistentStateException($"Database has invalid data for grain {grainReference}: data={data?.ToString() ?? "null"}, etag={eTag?.ToString() ?? "null"}, serializer code={serializerCode?.ToString() ?? "null"}");

            if (!options.SerializationProviders.TryGetValue((int)serializerCode, out var ProviderInfo))
                throw new BadProviderConfigException($"Serializer with code {serializerCode} has not been configured");

            grainState.State = ProviderInfo.provider.Deserialize(grainStateType, data);
            grainState.ETag = eTag.Value.ToString();
        }

        async Task IGrainStorage.WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainStateType = grainState.State.GetType();
            var typeInfo = GetTypeSerializationInfo(grainType, grainStateType);
            var keyBlob = GetKeyAsBlob(grainReference);

            var serializedData = typeInfo.providerInfo.provider.Serialize(grainState.State);
            var newETag = Guid.NewGuid();
            var oldETag = string.IsNullOrEmpty(grainState.ETag) ? default(Guid?) : Guid.Parse(grainState.ETag);

            var rows = await session.ExecuteAsync(queries.WriteToStorage(typeInfo.typeString, keyBlob, serializedData, (sbyte)typeInfo.providerInfo.code, newETag, oldETag));

            if (!((bool?)rows.FirstOrDefault()?["[applied]"] ?? false))
                throw new InconsistentStateException($"Failed to write grain {grainReference} due to version mismatch");

            grainState.ETag = newETag.ToString();
        }
    }
}
