using Cassandra;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using OrleansCassandraUtils.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace OrleansCassandraUtils.Clustering
{
    class CassandraGatewayListProvider : IGatewayListProvider
    {
        CassandraClusteringOptions options;
        TimeSpan maxStaleness;
        ISession session;
        OrleansQueries queries;


        TimeSpan IGatewayListProvider.MaxStaleness => maxStaleness;

        bool IGatewayListProvider.IsUpdatable => true;


        public CassandraGatewayListProvider(IOptions<CassandraClusteringOptions> options, IOptions<GatewayOptions> gatewayOptions)
        {
            this.options = options.Value;
            maxStaleness = gatewayOptions.Value.GatewayListRefreshPeriod;
        }

        async Task IGatewayListProvider.InitializeGatewayListProvider()
        {
            session = await CassandraSessionFactory.CreateSession(options.ConnectionString);
            queries = await OrleansQueries.CreateInstance(session);
        }

        async Task<IList<Uri>> IGatewayListProvider.GetGateways()
        {
            var rows = await session.ExecuteAsync(queries.GatewaysQuery((int)SiloStatus.Active));
            var result = new List<Uri>();

            foreach (var row in rows)
                result.Add(SiloAddress.New(new IPEndPoint(IPAddress.Parse((string)row["address"]), (int)row["proxy_port"]), (int)row["generation"]).ToGatewayUri());

            return result;
        }
    }
}
