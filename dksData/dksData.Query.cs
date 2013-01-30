using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace dksData
{
	public static partial class Database
	{
		
		public static IEnumerable<TRet> Query<TRet>(this IDbConnection db, string sql, params object[] parameters)
		{
			return new List<TRet>();
		}

	}
}