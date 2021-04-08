using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers;
using Orleans.Serialization;

namespace OrleansCassandraUtils.Persistence
{
    class DefaultSerializationProvider : IStorageSerializationProvider
    {
        SerializationManager serializationManager;

        public void Init(IProviderRuntime ProviderRuntime)
        {
            serializationManager = ProviderRuntime.ServiceProvider.GetRequiredService<SerializationManager>();
        }

        public string GetTypeString(string orleansTypeString, Type type)
        {
            return orleansTypeString;
        }

        public bool IsSupportedType(Type type)
        {
            return true;
        }

        public object Deserialize(Type expectedType, byte[] data)
        {
            return serializationManager.DeserializeFromByteArray<object>(data);
        }

        public byte[] Serialize(object @object)
        {
            return serializationManager.SerializeToByteArray(@object);
        }

        public object CreateInstance(Type type)
        {
            return Activator.CreateInstance(type);
        }
    }
}
