using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Orleans;
using Orleans.Runtime;
using OrleansCassandraUtils.Reminders;

namespace OrleansCassandraUtils.Utils
{
    /// <summary>
    /// This class is responsible for keeping a list of prepared queries and
    /// knowing their parameters (including type and conversion to the target 
    /// type). We could have another class which would be responsible for 
    /// knowledge of query result columns, but I don't see the benefit right
    /// now, specially since each query is used by one class only.
    /// </summary>
    internal class OrleansQueries : Queries
    {
        const int reminderPartitionBits = 6;


        public static new async Task<OrleansQueries> CreateInstance(ISession session)
        {
            await Queries.CreateInstance(session);
            return new OrleansQueries(session);
        }


        private OrleansQueries(ISession session) : base(session) { }


        static sbyte GetPartitionFromGrainHash(int grainDatabaseHash) => (sbyte)(grainDatabaseHash >> (32 - reminderPartitionBits));

        static int GetGrainHash(GrainReference grainRef) => GetGrainHashForDatabase(grainRef.GetUniformHashCode());

        static int GetGrainHashForDatabase(uint grainHash) => (int)(grainHash + int.MinValue);

        static (sbyte partition, int hash, byte[] grainKey) GetGrainKeys(GrainReference grainRef, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var hash = GetGrainHash(grainRef);
            return (GetPartitionFromGrainHash(hash), hash, grainReferenceConversionProvider.GetKey(grainRef));
        }

        internal IStatement UpsertReminderRow(ReminderEntry entry, Guid eTag, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var (partition, hash, grainKey) = GetGrainKeys(entry.GrainRef, grainReferenceConversionProvider);
            return this["UpsertReminderRowKey"].Bind(new
            {
                partition = partition,
                grain_hash = hash,
                grain_id = grainKey,
                reminder_name = entry.ReminderName,
                start_time = entry.StartAt,
                period = (int)entry.Period.TotalMilliseconds,
                etag = eTag,
            });
        }

        internal IStatement ReadReminderRow(GrainReference grainRef, string reminderName, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var (partition, hash, grainKey) = GetGrainKeys(grainRef, grainReferenceConversionProvider);
            return this["ReadReminderRowKey"].Bind(new
            {
                partition = partition,
                grain_hash = hash,
                grain_id = grainKey,
                reminder_name = reminderName
            });
        }

        internal IStatement ReadReminderRows(GrainReference grainRef, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var (partition, hash, grainKey) = GetGrainKeys(grainRef, grainReferenceConversionProvider);
            return this["ReadReminderRowsKey"].Bind(new
            {
                partition = partition,
                grain_hash = hash,
                grain_id = grainKey,
            });
        }

        internal IStatement DeleteReminderRows()
        {
            return this["DeleteReminderRowsKey"].Bind(null);
        }

        internal IStatement DeleteReminderRow(GrainReference grainRef, string reminderName, Guid expectedETag, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var Keys = GetGrainKeys(grainRef, grainReferenceConversionProvider);
            return this["DeleteReminderRowKey"].Bind(new
            {
                partition = Keys.Item1,
                grain_hash = Keys.Item2,
                grain_id = Keys.Item3,
                reminder_name = reminderName,
                etag = expectedETag
            });
        }

        sbyte[] GetPartitionsForRange(uint grainHashStart, uint grainHashEnd)
        {
            var firstPartition = GetPartitionFromGrainHash(GetGrainHashForDatabase(grainHashStart));
            var lastPartition = GetPartitionFromGrainHash(GetGrainHashForDatabase(grainHashEnd));

            var partitions = new sbyte[lastPartition - firstPartition + 1];
            for (sbyte i = firstPartition; i <= lastPartition; ++i)
                partitions[i - firstPartition] = i;

            return partitions;
        }

        internal IStatement ReadRemindersInsideRange(uint grainHashStart, uint grainHashEnd) =>
            this["ReadRemindersInsideRangeKey"].Bind(new
            {
                partitions = GetPartitionsForRange(grainHashStart, grainHashEnd),
                grain_hash_start = GetGrainHashForDatabase(grainHashStart),
                grain_hash_end = GetGrainHashForDatabase(grainHashEnd)
            });

        internal IStatement ReadRemindersOutsideRange1(uint grainHashStart) =>
            this["ReadRemindersOutsideRangeKey1"].Bind(new
            {
                partitions = GetPartitionsForRange(grainHashStart, uint.MaxValue),
                grain_hash_start = GetGrainHashForDatabase(grainHashStart)
            });

        internal IStatement ReadRemindersOutsideRange2(uint grainHashEnd) =>
            this["ReadRemindersOutsideRangeKey2"].Bind(new
            {
                partitions = GetPartitionsForRange(uint.MinValue, grainHashEnd),
                grain_hash_end = GetGrainHashForDatabase(grainHashEnd)
            });

        internal IStatement InsertMembership(MembershipEntry membershipEntry, int version) =>
            this["InsertMembershipKey"].Bind(new
            {
                address = membershipEntry.SiloAddress.Endpoint.Address.ToString(),
                port = membershipEntry.SiloAddress.Endpoint.Port,
                generation = membershipEntry.SiloAddress.Generation,
                silo_name = membershipEntry.SiloName,
                host_name = membershipEntry.HostName,
                status = (int)membershipEntry.Status,
                proxy_port = membershipEntry.ProxyPort,
                start_time = membershipEntry.StartTime,
                i_am_alive_time = membershipEntry.IAmAliveTime,
                new_version = version + 1,
                expected_version = version
            });

        internal IStatement InsertMembershipVersion() => this["InsertMembershipVersionKey"].Bind(null);

        internal IStatement DeleteMembershipTableEntries() => this["DeleteMembershipTableEntriesKey"].Bind(null);

        internal IStatement UpdateIAmAliveTime(MembershipEntry membershipEntry) =>
            this["UpdateIAmAliveTimeKey"].Bind(new
            {
                i_am_alive_time = membershipEntry.IAmAliveTime,
                address = membershipEntry.SiloAddress.Endpoint.Address.ToString(),
                port = membershipEntry.SiloAddress.Endpoint.Port,
                generation = membershipEntry.SiloAddress.Generation
            });

        internal IStatement DeleteMembershipEntry(MembershipEntry membershipEntry) =>
            this["DeleteMembershipEntryKey"].Bind(new
            {
                address = membershipEntry.SiloAddress.Endpoint.Address.ToString(),
                port = membershipEntry.SiloAddress.Endpoint.Port,
                generation = membershipEntry.SiloAddress.Generation
            });

        internal IStatement UpdateMembership(MembershipEntry membershipEntry, int version) =>
            this["UpdateMembershipKey"].Bind(new
            {
                new_version = version + 1,
                expected_version = version,
                status = (int)membershipEntry.Status,
                suspect_times = membershipEntry.SuspectTimes == null ? null : string.Join("|", membershipEntry.SuspectTimes.Select(s => $"{s.Item1.ToParsableString()},{LogFormatter.PrintDate(s.Item2)}")),
                i_am_alive_time = membershipEntry.IAmAliveTime,
                address = membershipEntry.SiloAddress.Endpoint.Address.ToString(),
                port = membershipEntry.SiloAddress.Endpoint.Port,
                generation = membershipEntry.SiloAddress.Generation
            });

        internal IStatement MembershipReadVersion() => this["MembershipReadVersionKey"].Bind(null);

        internal IStatement MembershipReadAll() => this["MembershipReadAllKey"].Bind(null);

        internal IStatement MembershipReadRow(SiloAddress siloAddress) =>
            this["MembershipReadRowKey"].Bind(new
            {
                address = siloAddress.Endpoint.Address.ToString(),
                port = siloAddress.Endpoint.Port,
                generation = siloAddress.Generation
            });

        internal IStatement GatewaysQuery(int status) => this["GatewaysQueryKey"].Bind(new { status = status });

        internal IStatement ReadFromStorage(string GrainType, byte[] GrainId) =>
            this["ReadFromStorageKey"].Bind(new
            {
                grain_type = GrainType,
                grain_id = GrainId
            });

        internal IStatement WriteToStorage(string GrainType, byte[] GrainId, byte[] Data, sbyte SerializerCode, Guid NewETag, Guid? ExpectedETag) =>
            this["WriteToStorageKey"].Bind(new
            {
                grain_type = GrainType,
                grain_id = GrainId,
                data = Data,
                serializer_code = SerializerCode,
                etag = NewETag,
                expected_etag = ExpectedETag
            });

        internal IStatement ClearStorage(string GrainType, byte[] GrainId, Guid ExpectedETag) =>
            this["ClearStorageKey"].Bind(new
            {
                grain_type = GrainType,
                grain_id = GrainId,
                expected_etag = ExpectedETag
            });
    }
}
