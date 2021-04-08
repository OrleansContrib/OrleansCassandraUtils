using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Runtime;
using OrleansCassandraUtils.Utils;

namespace OrleansCassandraUtils.Reminders
{
    class CassandraReminderTable : IReminderTable
    {
        CassandraReminderTableOptions options;
        IGrainReferenceConversionProvider grainReferenceConversionProvider;
        ISession session;
        OrleansQueries queries;


        public CassandraReminderTable(IGrainReferenceConverter grainReferenceConverter, IOptions<CassandraReminderTableOptions> options, IGrainReferenceConversionProvider grainReferenceConversionProvider = null)
        {
            this.options = options.Value;
            this.grainReferenceConversionProvider = grainReferenceConversionProvider ?? new DefaultGrainReferenceConversionProvider(grainReferenceConverter);
        }

        public async Task Init()
        {
            session = await CassandraSessionFactory.CreateSession(options.ConnectionString);
            queries = await OrleansQueries.CreateInstance(session);
        }

        ReminderEntry GetReminderEntry(Row row, GrainReference forGrain = null, string reminderName = null)
        {
            try
            {
                return new ReminderEntry()
                {
                    GrainRef = forGrain ?? grainReferenceConversionProvider.GetGrain((byte[])row["grain_id"]),
                    ReminderName = reminderName ?? (string)row["reminder_name"],
                    StartAt = ((DateTimeOffset)row["start_time"]).UtcDateTime,
                    Period = TimeSpan.FromMilliseconds((int)row["period"]),
                    ETag = row["etag"].ToString()
                };
            }
            catch
            {
                return null;
            }
        }

        IEnumerable<ReminderEntry> GetReminderEntries(RowSet rows, GrainReference forGrain = null, string reminderName = null)
        {
            var result = new List<ReminderEntry>();

            foreach (var row in rows)
            {
                var Entry = GetReminderEntry(row, forGrain, reminderName);
                if (Entry != null)
                    result.Add(Entry);
            }

            return result;
        }

        public async Task<ReminderEntry> ReadRow(GrainReference grainRef, string reminderName)
        {
            var rows = await session.ExecuteAsync(queries.ReadReminderRow(grainRef, reminderName, grainReferenceConversionProvider));

            return GetReminderEntries(rows, grainRef, reminderName).FirstOrDefault();
        }

        public async Task<ReminderTableData> ReadRows(GrainReference grainRef)
        {
            var rows = await session.ExecuteAsync(queries.ReadReminderRows(grainRef, grainReferenceConversionProvider));

            return new ReminderTableData(GetReminderEntries(rows, grainRef));
        }

        public async Task<ReminderTableData> ReadRows(uint begin, uint end)
        {
            if (begin < end)
            {
                var Rows = await session.ExecuteAsync(queries.ReadRemindersInsideRange(begin, end));

                return new ReminderTableData(GetReminderEntries(Rows));
            }
            else
            {
                var rows1 = await session.ExecuteAsync(queries.ReadRemindersOutsideRange1(begin));
                var rows2 = await session.ExecuteAsync(queries.ReadRemindersOutsideRange2(end));

                return new ReminderTableData(GetReminderEntries(rows1).Concat(GetReminderEntries(rows2)));
            }
        }

        public Task<bool> RemoveRow(GrainReference grainRef, string reminderName, string eTag)
        {
            return session.ExecuteAsync(queries.DeleteReminderRow(grainRef, reminderName, Guid.Parse(eTag), grainReferenceConversionProvider))
                .ContinueWith(t => (bool)t.Result.First()["[applied]"]);
        }

        public Task<string> UpsertRow(ReminderEntry entry)
        {
            var eTag = Guid.NewGuid();
            return session.ExecuteAsync(queries.UpsertReminderRow(entry, eTag, grainReferenceConversionProvider))
                .ContinueWith(t => t.IsCompleted ? eTag.ToString() : throw t.Exception);
        }

        public Task TestOnlyClearTable()
        {
            return session.ExecuteAsync(queries.DeleteReminderRows());
        }
    }
}
