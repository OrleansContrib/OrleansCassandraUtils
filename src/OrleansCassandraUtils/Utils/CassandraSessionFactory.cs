using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;

namespace OrleansCassandraUtils.Utils
{
    public static class CassandraSessionFactory
    {
        static object lockObj = new object();
        static Dictionary<string, Task<ISession>> sessionCache = new Dictionary<string, Task<ISession>>();


        public static Task<ISession> CreateSession(string connectionString)
        {
            lock (lockObj)
            {
                if (sessionCache.TryGetValue(connectionString, out var found))
                    return found;

                var builder = Cluster.Builder();

                string username = null, password = null;

                var options = connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var option in options)
                {
                    var Parts = option.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                    if (Parts.Length != 2)
                        throw new FormatException($"Invalid connection string option: {option}");

                    var Key = Parts[0].Trim().ToLower();
                    var Value = Parts[1].Trim();

                    switch (Key)
                    {
                        case "contact point":
                            builder.AddContactPoint(Value);
                            break;

                        case "username":
                            username = Value;
                            if (password != null)
                                builder.WithCredentials(username, password);
                            break;

                        case "password":
                            password = Value;
                            if (username != null)
                                builder.WithCredentials(username, password);
                            break;

                        case "keyspace":
                            builder.WithDefaultKeyspace(Value);
                            break;

                        case "compression":
                            if (Enum.TryParse(Value, true, out CompressionType CompressionType))
                                builder.WithCompression(CompressionType);
                            else
                                throw new FormatException($"Unknown compression type {Value}");
                            break;

                        case "port":
                            builder.WithPort(int.Parse(Value));
                            break;

                        case "ssl":
                            if (bool.Parse(Value) == true)
                                builder.WithSSL();
                            break;

                        default:
                            throw new FormatException($"Unknown key {Key}");
                    }
                }

                var result = builder.Build().ConnectAsync();
                sessionCache.Add(connectionString, result);
                return result;
            }
        }
    }
}
