using Orleans.Runtime;

namespace OrleansCassandraUtils.Reminders
{
    public interface IGrainReferenceConversionProvider
    {
        byte[] GetKey(GrainReference grainRef);
        GrainReference GetGrain(byte[] key);
    }
}
