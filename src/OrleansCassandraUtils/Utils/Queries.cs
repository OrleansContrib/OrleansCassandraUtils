using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace OrleansCassandraUtils.Utils
{
    public class Queries
    {
        static readonly SemaphoreSlim @lock = new SemaphoreSlim(1);
        static readonly string readQueriesCommand = "SELECT key, text, consistency_level FROM queries;";

        static readonly Dictionary<ISession, Dictionary<string, PreparedStatement>> queries = new Dictionary<ISession, Dictionary<string, PreparedStatement>>();


        public PreparedStatement this[string key] => queries[Session][key];

        public ISession Session { get; }


        public static async Task<Queries> CreateInstance(ISession session)
        {
            if (!queries.ContainsKey(session))
            {
                await @lock.WaitAsync();

                try
                {
                    if (!queries.ContainsKey(session))
                    {
                        var queryRows = await session.ExecuteAsync(new SimpleStatement(readQueriesCommand));

                        var dic = new Dictionary<string, PreparedStatement>();
                        foreach (var Row in queryRows)
                        {

                            var statement = await session.PrepareAsync((string)Row["text"]);
                            statement.SetConsistencyLevel((ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), (string)Row["consistency_level"], true));
                            dic.Add(Row["key"].ToString(), statement);
                        }

                        queries[session] = dic;
                    }
                }
                finally
                {
                    @lock.Release();
                }
            }

            return new Queries(session);
        }


        protected Queries(ISession session) => Session = session;
    }
}
