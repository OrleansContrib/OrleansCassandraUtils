using Cassandra;
using Orleans;
using Orleans.Runtime;
using OrleansCassandraUtils.Reminders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OrleansCassandraUtils.Utils
{
    /// <summary>
    /// This class is responsible for keeping a list of prepared queries and
    /// knowing their parameters (including type and conversion to the target 
    /// type). We could have another class which would be responsible for 
    /// knowledge of query result columns, but I don't see the benefit right
    /// now, specially since each query is used by one class only.
    /// </summary>
    class OrleansQueries
    {
        const int reminderPartitionBits = 6;

        static SemaphoreSlim @lock = new SemaphoreSlim(1);
        static string readQueriesCommand = "SELECT key, text, consistency_level FROM queries;";

        static Dictionary<string, PreparedStatement> queries = null;


        public static async Task<OrleansQueries> CreateInstance(ISession session)
        {
            await @lock.WaitAsync();

            try
            {
                if (queries == null)
                {
                    var queryRows = await session.ExecuteAsync(new SimpleStatement(readQueriesCommand));

                    var dic = new Dictionary<string, PreparedStatement>();
                    foreach (var Row in queryRows)
                    {

                        var statement = await session.PrepareAsync((string)Row["text"]);
                        statement.SetConsistencyLevel((ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), (string)Row["consistency_level"], true));
                        dic.Add(Row["key"].ToString(), statement);
                    }

                    queries = dic;
                }

                return new OrleansQueries();
            }
            finally
            {
                @lock.Release();
            }
        }


        private OrleansQueries() { }


        static sbyte GetPartitionFromGrainHash(int grainDatabaseHash)
        {
            return (sbyte)(grainDatabaseHash >> (32 - reminderPartitionBits));
        }

        static int GetGrainHash(GrainReference grainRef)
        {
            return GetGrainHashForDatabase(grainRef.GetUniformHashCode());
        }

        static int GetGrainHashForDatabase(uint grainHash)
        {
            return (int)(grainHash + int.MinValue);
        }

        static Tuple<sbyte, int, byte[]> GetGrainKeys(GrainReference grainRef, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var hash = GetGrainHash(grainRef);
            return new Tuple<sbyte, int, byte[]>(GetPartitionFromGrainHash(hash), hash, grainReferenceConversionProvider.GetKey(grainRef));
        }

        public IStatement UpsertReminderRow(ReminderEntry entry, Guid eTag, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var Keys = GetGrainKeys(entry.GrainRef, grainReferenceConversionProvider);
            return queries["UpsertReminderRowKey"].Bind(new
            {
                partition = Keys.Item1,
                grain_hash = Keys.Item2,
                grain_id = Keys.Item3,
                reminder_name = entry.ReminderName,
                start_time = entry.StartAt,
                period = (int)entry.Period.TotalMilliseconds,
                etag = eTag,
            });
        }

        public IStatement ReadReminderRow(GrainReference grainRef, string reminderName, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var Keys = GetGrainKeys(grainRef, grainReferenceConversionProvider);
            return queries["ReadReminderRowKey"].Bind(new
            {
                partition = Keys.Item1,
                grain_hash = Keys.Item2,
                grain_id = Keys.Item3,
                reminder_name = reminderName
            });
        }

        public IStatement ReadReminderRows(GrainReference grainRef, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var Keys = GetGrainKeys(grainRef, grainReferenceConversionProvider);
            return queries["ReadReminderRowsKey"].Bind(new
            {
                partition = Keys.Item1,
                grain_hash = Keys.Item2,
                grain_id = Keys.Item3,
            });
        }

        public IStatement DeleteReminderRows()
        {
            return queries["DeleteReminderRowsKey"].Bind(null);
        }

        public IStatement DeleteReminderRow(GrainReference grainRef, string reminderName, Guid expectedETag, IGrainReferenceConversionProvider grainReferenceConversionProvider)
        {
            var Keys = GetGrainKeys(grainRef, grainReferenceConversionProvider);
            return queries["DeleteReminderRowKey"].Bind(new
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
            var FirstPartition = GetPartitionFromGrainHash(GetGrainHashForDatabase(grainHashStart));
            var LastPartition = GetPartitionFromGrainHash(GetGrainHashForDatabase(grainHashEnd));

            var Partitions = new sbyte[LastPartition - FirstPartition + 1];
            for (sbyte i = FirstPartition; i <= LastPartition; ++i)
                Partitions[i - FirstPartition] = i;

            return Partitions;
        }

        public IStatement ReadRemindersInsideRange(uint grainHashStart, uint grainHashEnd)
        {
            return queries["ReadRemindersInsideRangeKey"].Bind(new
            {
                partitions = GetPartitionsForRange(grainHashStart, grainHashEnd),
                grain_hash_start = GetGrainHashForDatabase(grainHashStart),
                grain_hash_end = GetGrainHashForDatabase(grainHashEnd)
            });
        }

        public IStatement ReadRemindersOutsideRange1(uint grainHashStart)
        {
            return queries["ReadRemindersOutsideRangeKey1"].Bind(new
            {
                partitions = GetPartitionsForRange(grainHashStart, uint.MaxValue),
                grain_hash_start = GetGrainHashForDatabase(grainHashStart)
            });
        }

        public IStatement ReadRemindersOutsideRange2(uint grainHashEnd)
        {
            return queries["ReadRemindersOutsideRangeKey2"].Bind(new
            {
                partitions = GetPartitionsForRange(uint.MinValue, grainHashEnd),
                grain_hash_end = GetGrainHashForDatabase(grainHashEnd)
            });
        }

        public IStatement InsertMembership(MembershipEntry membershipEntry, int version)
        {
            return queries["InsertMembershipKey"].Bind(new
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
        }

        public IStatement InsertMembershipVersion()
        {
            return queries["InsertMembershipVersionKey"].Bind(null);
        }

        public IStatement DeleteMembershipTableEntries()
        {
            return queries["DeleteMembershipTableEntriesKey"].Bind(null);
        }

        public IStatement UpdateIAmAliveTime(MembershipEntry membershipEntry)
        {
            return queries["UpdateIAmAliveTimeKey"].Bind(new
            {
                i_am_alive_time = membershipEntry.IAmAliveTime,
                address = membershipEntry.SiloAddress.Endpoint.Address.ToString(),
                port = membershipEntry.SiloAddress.Endpoint.Port,
                generation = membershipEntry.SiloAddress.Generation
            });
        }

        public IStatement UpdateMembership(MembershipEntry membershipEntry, int version)
        {
            return queries["UpdateMembershipKey"].Bind(new
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
        }

        public IStatement MembershipReadVersion()
        {
            return queries["MembershipReadVersionKey"].Bind(null);
        }

        public IStatement MembershipReadAll()
        {
            return queries["MembershipReadAllKey"].Bind(null);
        }

        public IStatement MembershipReadRow(SiloAddress siloAddress)
        {
            return queries["MembershipReadRowKey"].Bind(new
            {
                address = siloAddress.Endpoint.Address.ToString(),
                port = siloAddress.Endpoint.Port,
                generation = siloAddress.Generation
            });
        }

        public IStatement GatewaysQuery(int status)
        {
            return queries["GatewaysQueryKey"].Bind(new
            {
                status = status
            });
        }

        public IStatement ReadFromStorage(string GrainType, byte[] GrainId)
        {
            return queries["ReadFromStorageKey"].Bind(new
            {
                grain_type = GrainType,
                grain_id = GrainId
            });
        }

        public IStatement WriteToStorage(string GrainType, byte[] GrainId, byte[] Data, sbyte SerializerCode, Guid NewETag, Guid? ExpectedETag)
        {
            return queries["WriteToStorageKey"].Bind(new
            {
                grain_type = GrainType,
                grain_id = GrainId,
                data = Data,
                serializer_code = SerializerCode,
                etag = NewETag,
                expected_etag = ExpectedETag
            });
        }

        public IStatement ClearStorage(string GrainType, byte[] GrainId, Guid ExpectedETag)
        {
            return queries["ClearStorageKey"].Bind(new
            {
                grain_type = GrainType,
                grain_id = GrainId,
                expected_etag = ExpectedETag
            });
        }
    }
}
