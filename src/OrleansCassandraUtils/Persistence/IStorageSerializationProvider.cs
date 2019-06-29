using Orleans;
using Orleans.Providers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
