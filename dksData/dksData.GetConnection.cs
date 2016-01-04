/*
   Copyright 2013 David Smith

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace dksData
{
	public static partial class Database
	{

		// todo: Implement cache expiry, clean up etc...
		// todo: implement parameters for commandTimeout, commandType, openTransaction, CommandBehavior type...?
		// todo: handle different variable prefixs in sql string. ie MySQL uses ? and Oracle use a :
		// todo: implement fill dataset?

		// cache functions to create appropriate DbConnection.
		private static ConcurrentDictionary<string, Func<DbConnection>> dbFactoryCache = new ConcurrentDictionary<string, Func<DbConnection>>();


		public static DbConnection GetOpenConnection(string connectionStringName)
		{
			var db = GetConnection(connectionStringName);
			db.Open();
			return db;
		}

		public static async Task<DbConnection> GetOpenConnectionAsync(string connectionStringName)
		{
			var db = GetConnection(connectionStringName);
			await db.OpenAsync();
			return db;
		}



		public static DbConnection GetConnection(string connectionStringName)
		{
			Func<DbConnection> createConnection;

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
						DbConnection db;
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