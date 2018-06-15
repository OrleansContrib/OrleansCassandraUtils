using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace OrleansCassandraUtils.Reminders
{
    public interface IGrainReferenceConversionProvider
    {
        byte[] GetKey(GrainReference grainRef);
        GrainReference GetGrain(byte[] key);
    }
}
