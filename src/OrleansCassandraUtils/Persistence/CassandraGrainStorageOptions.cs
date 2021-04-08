using System.Collections.Generic;
using Orleans;
using Orleans.Storage;

namespace OrleansCassandraUtils.Persistence
{
    public class CassandraGrainStorageOptions
    {
        internal class SerializationProviderInfo
        {
            public int code;
            public IStorageSerializationProvider provider;
        }

        [RedactConnectionString]
        public string ConnctionString { get; set; }

        public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;

        internal SortedDictionary<int, SerializationProviderInfo> SerializationProviders { get; } = new SortedDictionary<int, SerializationProviderInfo>();


        public CassandraGrainStorageOptions()
        {
            SerializationProviders.Add(127, new SerializationProviderInfo { code = 127, provider = new DefaultSerializationProvider() });
        }

        public void AddSerializationProvider(int code, IStorageSerializationProvider provider)
        {
            if (code == 127)
                throw new BadProviderConfigException("Code 127 is reserved for the default serialization provider.");

            if (SerializationProviders.ContainsKey(code))
                throw new BadProviderConfigException($"Duplicate code {code} between {provider.GetType()} and {SerializationProviders[code].GetType()}");

            SerializationProviders.Add(code, new SerializationProviderInfo { code = code, provider = provider });
        }
    }
}
