using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace dksData
{
    public static sealed class Database
    {

        // todo: Implement cache clean expiry, clean up etc...


        #region "IDbConnection GetConnection(...)"

        // cache functions to create appropriate IDbConnection.
        private static ConcurrentDictionary<string, Func<IDbConnection>> dbFactoryCache = new ConcurrentDictionary<string, Func<IDbConnection>>();

        public static IDbConnection GetConnection()
        {
            string connectionStringName;

            connectionStringName = ConfigurationManager.ConnectionStrings[0].Name;

            return GetConnection(connectionStringName);
        }

        public static IDbConnection GetConnection(string connectionStringName)
        {
            Func<IDbConnection> createConnection;

            if (ConfigurationManager.ConnectionStrings[connectionStringName] == null)
            {
                throw new InvalidOperationException("Can't find a connection string with the name '" + connectionStringName + "'");
            }

            // look in cache for factory method
            if (dbFactoryCache.TryGetValue(connectionStringName.ToLower(), out createConnection) == false)
            {
                // not in cache, build dynamic function and store in cache
                ConnectionStringSettings c;
                c = ConfigurationManager.ConnectionStrings[connectionStringName];

                if (string.IsNullOrWhiteSpace(c.ProviderName) == true || c.ProviderName.ToLower() == "system.data.sqlclient")
                {
                    createConnection = () =>
                    {
                        IDbConnection db;
                        db = new SqlConnection();
                        db.ConnectionString = c.ConnectionString;
                        return db;
                    };
                }
                else
                {
                    createConnection = () =>
                    {
                        var dbf = DbProviderFactories.GetFactory(c.ProviderName);
                        var db = dbf.CreateConnection();
                        db.ConnectionString = c.ConnectionString;
                        return db;
                    };
                }

                dbFactoryCache[connectionStringName.ToLower()] = createConnection;

            }

            return createConnection();


        }

        #endregion
    }
}
