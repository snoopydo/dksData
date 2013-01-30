using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace dksData
{
	public static partial class Database
	{

		// todo: Implement cache expiry, clean up etc...
		// todo: implement parameters for commandTimeout, commandType, openTransaction, CommandBehavior type...?
		// todo: handle different variable prefixs in sql string. ie MySQL uses ? and Oracle use a :
		// todo: implement fill dataset?

		// cache functions to create appropriate IDbConnection.
		private static ConcurrentDictionary<string, Func<IDbConnection>> dbFactoryCache = new ConcurrentDictionary<string, Func<IDbConnection>>();


		public static IDbConnection GetOpenConnection(string connectionStringName)
		{
			var db = GetConnection(connectionStringName);
			db.Open();

			return db;
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

				var connectionSettings = ConfigurationManager.ConnectionStrings[connectionStringName];

				if (string.IsNullOrWhiteSpace(connectionSettings.ProviderName) == true || connectionSettings.ProviderName.ToLower() == "system.data.sqlclient")
				{
					createConnection = () =>
					{
						IDbConnection db;
						db = new SqlConnection();
						db.ConnectionString = connectionSettings.ConnectionString;
						return db;
					};
				}
				else
				{
					createConnection = () =>
					{
						var dbf = DbProviderFactories.GetFactory(connectionSettings.ProviderName);
						var db = dbf.CreateConnection();
						db.ConnectionString = connectionSettings.ConnectionString;
						return db;
					};
				}

				dbFactoryCache[connectionStringName.ToLower()] = createConnection;

			}

			return createConnection();


		}

	}
}