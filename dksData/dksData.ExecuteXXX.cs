using System;
using System.Data;

namespace dksData
{
	public static partial class Database
	{

		public static T ExecuteScalar<T>(this IDbConnection db, string sql, params object[] parameters)
		{

			using (var cmd = CreateCommand(db, sql, parameters))
			{
				//cmd.CommandTimeout = commandTimeout;
				//cmd.CommandType = commandType;
				//cmd.Transaction = transaction;
				object result;

				result = cmd.ExecuteScalar();

				return (T)Convert.ChangeType(result, typeof(T));

			}

		}

		public static int ExecuteNonQuery(this IDbConnection db, string sql, params object[] parameters)
		{
			using (var cmd = CreateCommand(db, sql, parameters))
			{
				//cmd.CommandTimeout = commandTimeout;
				//cmd.CommandType = commandType;
				//cmd.Transaction = transaction;

				return cmd.ExecuteNonQuery();

			}
		}

		public static IDataReader ExecuteReader(this IDbConnection db, string sql, params object[] parameters)
		{
			using (var cmd = CreateCommand(db, sql, parameters))
			{
				//cmd.CommandTimeout = commandTimeout;
				//cmd.CommandType = commandType;
				//cmd.Transaction = transaction;
				// pass in CommandBehavior

				return cmd.ExecuteReader(CommandBehavior.CloseConnection);

			}
		}

	}
}
