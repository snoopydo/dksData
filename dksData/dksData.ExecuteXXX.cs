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
using System.Data;

namespace dksData
{
	public static partial class Database
	{

		public static T ExecuteScalar<T>(this IDbConnection db, string sql, params object[] parameters)
		{
			return ExecuteScalarWithTransaction<T>(db, null, sql, parameters);
		}

		public static T ExecuteScalarWithTransaction<T>(this IDbConnection db, IDbTransaction transaction, string sql, params object[] parameters)
		{

			using (var cmd = CreateCommand(db, transaction, sql, parameters))
			{
				object result;

				result = cmd.ExecuteScalar();

				return (T)Convert.ChangeType(result, typeof(T));

			}

		}

		public static int ExecuteNonQuery(this IDbConnection db, string sql, params object[] parameters)
		{
			return ExecuteNonQueryWithTransaction(db, null, sql, parameters);
		}

		public static int ExecuteNonQueryWithTransaction(this IDbConnection db, IDbTransaction transaction, string sql, params object[] parameters)
		{
			using (var cmd = CreateCommand(db, transaction,sql, parameters))
			{
				return cmd.ExecuteNonQuery();
			}
		}
		
		public static IDataReader ExecuteReader(this IDbConnection db, string sql, params object[] parameters)
		{
			return ExecuteReaderWithTransaction(db, null, sql, parameters);
		}

		public static IDataReader ExecuteReaderWithTransaction(this IDbConnection db, IDbTransaction transaction, string sql, params object[] parameters)
		{
			using (var cmd = CreateCommand(db, transaction,sql, parameters))
			{
				// pass in CommandBehavior
				return cmd.ExecuteReader(CommandBehavior.CloseConnection);
			}
		}

	}
}
