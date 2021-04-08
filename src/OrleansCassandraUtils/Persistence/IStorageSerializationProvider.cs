using System;
using Orleans.Providers;

namespace OrleansCassandraUtils.Persistence
{
    public interface IStorageSerializationProvider
    {
        void Init(IProviderRuntime providerRuntime);
        string GetTypeString(string orleansTypeString, Type type);
        bool IsSupportedType(Type type);
        object Deserialize(Type expectedType, byte[] data);
        byte[] Serialize(object @object);
        object CreateInstance(Type type);
    }
}
