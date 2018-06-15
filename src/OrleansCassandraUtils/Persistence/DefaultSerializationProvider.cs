using Orleans.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Microsoft.Extensions.DependencyInjection;

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
    }
}
