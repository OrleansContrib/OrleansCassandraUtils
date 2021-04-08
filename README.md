# OrleansCassandraUtils

... is a library for MSR Orleans integration with Cassandra DB and it supports grain persistence, clustering and reminder table. Cassandra with its linear scale-out capability is a perfect fit for Orleans.

## How to use

OrleansCassandraUtils is Orleans 3.x compatible.

To set it up, you first need a working Cassandra DB. Create a new keyspace and run the `InitializeOrleansDatabase.cql` script on it. Note that unlike the ADO.Net provider, you can't put data from multiple clusters into one database/keyspace. You must create a new keyspace for each cluster.

OrleansCassandraUtils uses a custom connection string format not unlike SQL Server connection strings. The parameters (case-insensitive) are as follows:

* `contact point`: Address of Cassandra cluster contact point. You may specify this parameter multiple times. Only one available contact point is needed though, as the driver automatically discovers all cluster nodes after the first node is contacted. At least one is required.
* `keyspace`: Name of the keyspace to use initially. OrleansCassandraUtils will always use this keyspace. However, if you use `CassandraSessionFactory` (more on that later) to make custom queries against your Cassandra cluster, you may also use other keyspaces in your queries. Required.
* `username`: Name of Cassandra user or role to log in to the server as. If your cluster does not require authentication (not recommended for obvious reasons) you may leave this out.
* `password`: Password of the user or role. Optional.
* `compression`: May be one of `LZ4`, `Snappy` or `NoCompression`. Specifies connection compression settings. Optional.
* `port`: Port to connect to. Optional.
* `ssl`: May be one of `true` or `false`. Specifies connection encryption settings. Optional.
* Sample connection string: `Contact Point=localhost;KeySpace=my_orleans_keyspace;Compression=LZ4;Username=my_user;Password=mY_$@fe_P@sS\/\/orD`

Then, setup your silo as follows:

### Grain storage

```csharp
siloBuilder.AddCassandraGrainStorageAsDefault((CassandraGrainStorageOptions o) =>
{
    o.ConnctionString = myConnectionString;
    o.AddSerializationProvider(1, new MyCustomSerializationProvider());
})
```

You'll notice the `AddSerializationProvider` call above is unfamiliar. This is an optional feature you can use to provide your own serialization provider, either based on existing providers in Orleans or a new one altogether. You may add up to 127 providers, each with a unique code between 0 and 126. These codes must not change during the entire lifetime of a cluster, as the codes are stored alongside the data and then used to decide which deserializer to use when reading data back from the database. If you don't provide any custom serializers, Orleans' default serializer will be used. Beware however that the default serializer is not version-tolerant and you *will* break your entire DB if you make a change to any grain state classes. To implement a custom serializer, simply implement the `OrleansCassandraUtils.Persistence.IStorageSerializationProvider` interface.

It's also worth mentioning that all grain state is serialized as binary data, since I find XML and JSON to be very inefficient, space- and perfomance-wise ([see](http://geekswithblogs.net/LeonidGaneline/archive/2015/05/06/serializers-in-.net.-v.2.aspx) [here](https://auth0.com/blog/beating-json-performance-with-protobuf/) for example). If you need text serialization, you can turn the resulting string into a UTF-8 byte sequence. I prefer [Bond](https://github.com/Microsoft/bond), but you can also use [Protobuf](https://developers.google.com/protocol-buffers/docs/overview).

### Clustering

On the silo side:
```csharp
siloBuilder.UseCassandraClustering((CassandraClusteringOptions o) =>
{
    o.ConnectionString = myConnectionString;
})
```

and on the client side:

```csharp
clientBuilder.UseCassandraClustering((CassandraClusteringOptions o) =>
{
    o.ConnectionString = myConnectionString;
})
```

### Reminders

```csharp
siloBuilder.UseCassandraReminderService((CassandraClusteringOptions o) =>
{
    o.ConnectionString = myConnectionString;
})
```

### CassandraSessionFactory

Cassandra clusters can be a good place to store your non-grain data as well. OrleansCassandraUtils provides the `OrleansCassandraUtils.Utils.CassandraSessionFactory` class, which you may use to acquire a Cassandra session and make custom queries against your cluster as follows:

```csharp
var session = await CassandraSessionFactory.CreateSession(myConnectionString);
var queryResults = await Session.ExecuteAsync(new SimpleStatement("select * from my_table"));
```
