using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Runtime;
using OrleansCassandraUtils.Utils;

namespace OrleansCassandraUtils.Clustering
{
    class CassandraClusteringTable : IMembershipTable
    {
        CassandraClusteringOptions options;
        ISession session;
        OrleansQueries queries;


        public CassandraClusteringTable(IOptions<CassandraClusteringOptions> options)
        {
            this.options = options.Value;
        }

        async Task IMembershipTable.InitializeMembershipTable(bool tryInitTableVersion)
        {
            session = await CassandraSessionFactory.CreateSession(options.ConnectionString);
            queries = await OrleansQueries.CreateInstance(session);

            if (tryInitTableVersion)
                await session.ExecuteAsync(queries.InsertMembershipVersion());
        }

        Task IMembershipTable.DeleteMembershipTableEntries(string deploymentId)
        {
            return session.ExecuteAsync(queries.DeleteMembershipTableEntries());
        }

        Task<bool> IMembershipTable.InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            return session.ExecuteAsync(queries.InsertMembership(entry, tableVersion.Version - 1)).ContinueWith(t => (bool)t.Result.First()["[applied]"]);
        }

        Task<bool> IMembershipTable.UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            return session.ExecuteAsync(queries.UpdateMembership(entry, tableVersion.Version - 1)).ContinueWith(t => (bool)t.Result.First()["[applied]"]);
        }

        MembershipEntry GetMembershipEntry(Row row, SiloAddress forAddress = null)
        {
            if (row["start_time"] == null)
                return null;

            var result = new MembershipEntry
            {
                SiloAddress = forAddress ?? SiloAddress.New(new IPEndPoint(IPAddress.Parse((string)row["address"]), (int)row["port"]), (int)row["generation"]),
                SiloName = (string)row["silo_name"],
                HostName = (string)row["host_name"],
                Status = (SiloStatus)(int)row["status"],
                ProxyPort = (int)row["proxy_port"],
                StartTime = ((DateTimeOffset)row["start_time"]).UtcDateTime,
                IAmAliveTime = ((DateTimeOffset)row["i_am_alive_time"]).UtcDateTime
            };

            var suspectingSilos = (string)row["suspect_times"];
            if (!string.IsNullOrWhiteSpace(suspectingSilos))
            {
                result.SuspectTimes = new List<Tuple<SiloAddress, DateTime>>();
                result.SuspectTimes.AddRange(suspectingSilos.Split('|').Select(s =>
                {
                    var split = s.Split(',');
                    return new Tuple<SiloAddress, DateTime>(SiloAddress.FromParsableString(split[0]), LogFormatter.ParseDate(split[1]));
                }));
            }

            return result;
        }

        async Task<MembershipTableData> GetMembershipTableData(RowSet Rows, SiloAddress ForAddress = null)
        {
            int version;

            var firstRow = Rows.FirstOrDefault();
            if (firstRow != null)
            {
                version = (int)firstRow["version"];

                var entries = new List<Tuple<MembershipEntry, string>>();
                foreach (var row in new[] { firstRow }.Concat(Rows))
                {
                    var entry = GetMembershipEntry(row, ForAddress);
                    if (entry != null)
                        entries.Add(new Tuple<MembershipEntry, string>(entry, string.Empty));
                }

                return new MembershipTableData(entries, new TableVersion(version, version.ToString()));
            }
            else
            {
                version = (int)(await session.ExecuteAsync(queries.MembershipReadVersion())).First()["version"];
                return new MembershipTableData(new List<Tuple<MembershipEntry, string>>(), new TableVersion(version, version.ToString()));
            }
        }

        async Task<MembershipTableData> IMembershipTable.ReadAll()
        {
            return await GetMembershipTableData(await session.ExecuteAsync(queries.MembershipReadAll()));
        }

        async Task<MembershipTableData> IMembershipTable.ReadRow(SiloAddress key)
        {
            return await GetMembershipTableData(await session.ExecuteAsync(queries.MembershipReadRow(key)), key);
        }

        Task IMembershipTable.UpdateIAmAlive(MembershipEntry entry)
        {
            return session.ExecuteAsync(queries.UpdateIAmAliveTime(entry));
        }

        public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
        {
            var allEntries = (await session.ExecuteAsync(queries.MembershipReadAll())).Select(r => GetMembershipEntry(r));

            foreach (var e in allEntries)
                if (e.Status == SiloStatus.Dead && new DateTime(Math.Max(e.IAmAliveTime.Ticks, e.StartTime.Ticks)) < beforeDate)
                    await session.ExecuteAsync(queries.DeleteMembershipEntry(e));
        }
    }
}
