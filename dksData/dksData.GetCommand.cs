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

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
		public static IDbCommand CreateCommand(IDbConnection db, IDbTransaction transaction, string sql, params object[] parameters)
		{
			IDbCommand cmd;

			// handle named/numbered etc parameters, fixing sql if required.
			var new_parameters = new List<object>();
			sql = ParseParameters(sql, parameters, new_parameters);
			parameters = new_parameters.ToArray();

			sql = sql.Replace("@@", "@");	// remove double escaped

			cmd = db.CreateCommand();

			cmd.CommandText = sql;
			//cmd.CommandTimeout = commandTimeout;
			//cmd.CommandType = commandType;

			if (transaction != null)
			{
				cmd.Transaction = transaction;
			}


			foreach (var param in parameters)
			{
				AddParameter(cmd, param);
			}

			return cmd;
		}

		private static void AddParameter(IDbCommand cmd, object param)
		{

			IDbDataParameter p;
			p = param as IDbDataParameter;

			if (p != null)
			{
				cmd.Parameters.Add(p);
				return;
			}

			p = cmd.CreateParameter();

			p.ParameterName = cmd.Parameters.Count.ToString();
			if (param == null)
			{
				p.Value = DBNull.Value;
			}
			else
			{
				p.Value = param;

				// make strings a consistent size, helps with query plan caching.
				if (param.GetType() == typeof(string) && p.Size < 4000)
				{
					p.Size = 4000;
				}
			}

			cmd.Parameters.Add(p);

		}


		// flogged from TopTenSoftwares' PetaPoco and tweeked to handle parameters that are already of type IDbDataParameter
		private static Regex rxParams = new Regex(@"(?<!@)@\w+", RegexOptions.Compiled);
		private static string ParseParameters(string sql, object[] parameters, List<object> new_parameters)
		{
			return rxParams.Replace(sql, m =>
			{
				string param = m.Value.Substring(1);

				object arg_val;

				int paramIndex;
				if (int.TryParse(param, out paramIndex))
				{
					// Numbered parameter
					if (paramIndex < 0 || paramIndex >= parameters.Length)
						throw new ArgumentOutOfRangeException(string.Format("Parameter '@{0}' specified but only {1} parameters supplied (in `{2}`)", paramIndex, parameters.Length, sql));

					arg_val = parameters[paramIndex];
				}
				else
				{
					// Look for a property on one of the arguments with this name
					bool found = false;
					arg_val = null;
					foreach (var o in parameters)
					{
						// find actual property name, could be different case to that was used in query.
						foreach (var prop in o.GetType().GetProperties())
						{
							if (prop.Name.ToLower() == param.ToLower())
							{
								param = prop.Name;
								break;
							}
						}

						var pi = o.GetType().GetProperty(param);
						if (pi != null)
						{
							arg_val = pi.GetValue(o, null);
							found = true;
							break;
						}
					}

					if (!found)
					{
						// is parameter a IDbDataParameter?
						IDbDataParameter dbP;
						foreach (var o in parameters)
						{
							dbP = o as IDbDataParameter;
							if (dbP != null)
							{
								if (dbP.ParameterName.ToLower() == param.ToLower())
								{
									new_parameters.Add(o);
									return "@" + param;
								}
							}
						}
					}

					if (!found)
						throw new ArgumentException(string.Format("Parameter '@{0}' specified but none of the passed arguments have a property with this name (in '{1}')", param, sql));
				}

				// Expand collections to parameter lists
				if ((arg_val as System.Collections.IEnumerable) != null &&
					(arg_val as string) == null &&
					(arg_val as byte[]) == null)
				{
					var sb = new StringBuilder();
					foreach (var i in arg_val as System.Collections.IEnumerable)
					{
						sb.Append((sb.Length == 0 ? "@" : ",@") + new_parameters.Count.ToString());
						new_parameters.Add(i);
					}
					return sb.ToString();
				}
				else
				{
					new_parameters.Add(arg_val);
					return "@" + (new_parameters.Count - 1).ToString();
				}
			}
			);
		}


		// Get fields & properties that we can assign to.  
		// We could extend these to look at custom attributes to handle mapping names to fields from database queries.

		public class settableProperty
		{
			public string Name;
			public Type PropertyType;
			public MethodInfo Setter;
		}

		public class settableField
		{
			public string Name;
			public Type PropertyType;
			public FieldInfo Setter;
		}


		private static List<settableProperty> GetSettableProperties(Type iType)
		{
			var properties = iType
					.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
					.Select(p => new settableProperty
					{
						Name = p.Name,
						Setter = p.DeclaringType == iType ? p.GetSetMethod(true) : p.DeclaringType.GetProperty(p.Name).GetSetMethod(true),
						PropertyType = p.PropertyType
					})
					.Where(info => info.Setter != null)
					.ToList();


			return properties;
		}

		private static List<settableField> GetSettableFields(Type iType)
		{
			var fields = iType.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
					 .Select(f => new settableField
					 {
						 Name = f.Name,
						 Setter = f,
						 PropertyType = f.FieldType
					 })
					 .ToList();


			return fields;
		}


	}
}
