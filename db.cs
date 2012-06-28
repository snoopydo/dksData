using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Data
{
	/// <summary>
	/// Summary description for db
	/// </summary>

	public static class DB
	{
		//todo: document methods.



		#region "public static IEnumerable<T> Query<T>(...) overloads"
		/// <summary>
		/// Executes the SQL using the default connection string 'sqlConnection' and returns an IEnumerable of <typeparamref name="T"/>
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="parameters">Optional list of named parameters</param>
		/// <returns>IEnumerable <typeparamref name="T"/></returns>
		public static IEnumerable<T> Query<T>(string sql, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(defaultConnection, CommandType.Text, sql, 0, parameters);
		}

		/// <summary>
		/// Executes the SQL using the default connection string 'sqlConnection' and returns an IEnumerable of <typeparamref name="T"/>
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="timeout">Timeout value for executing then command</param>
		/// <param name="parameters">Optional list of named parameters</param>
		/// <returns>IEnumerable <typeparamref name="T"/></returns>
		public static IEnumerable<T> Query<T>(string sql, int timeout, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(defaultConnection, CommandType.Text, sql, timeout, parameters);
		}

		/// <summary>
		/// Executes the SQL using the specified connection string and returns an IEnumerable of <typeparamref name="T"/>
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="connection">Name of connection string specified in the Connection Strings section of app.settings/web.config</param>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="parameters">Optional list of named parameters</param>
		/// <returns>IEnumerable <typeparamref name="T"/></returns>
		public static IEnumerable<T> Query<T>(string connection, string sql, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(connection, CommandType.Text, sql, 0, parameters);
		}

		/// <summary>
		/// Executes the SQL using the specified connection string and returns an IEnumerable of <typeparamref name="T"/>
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="connection">Name of connection string specified in the Connection Strings section of app.settings/web.config</param>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="timeout">Timeout value for executing then command</param>
		/// <param name="parameters">Optional list of named parameters</param>
		/// <returns>IEnumerable <typeparamref name="T"/></returns>
		public static IEnumerable<T> Query<T>(string connection, string sql, int timeout, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(connection, CommandType.Text, sql, timeout, parameters);
		}

		/// <summary>
		/// Executes the SQL using the default connection string 'sqlConnection' and returns an IEnumerable of <typeparamref name="T"/> 
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="type"><typeparamref name="System.Data.CommandType"/></param>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="parameters">>Optional list of named parameters</param>
		/// <returns></returns>
		public static IEnumerable<T> Query<T>(CommandType type, string sql, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(defaultConnection, type, sql, 0, parameters);
		}

		/// <summary>
		/// Executes the SQL using the default connection string 'sqlConnection' and returns an IEnumerable of <typeparamref name="T"/> 
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="type"><typeparamref name="System.Data.CommandType"/></param>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="timeout">Timeout value for executing then command</param>
		/// <param name="parameters">>Optional list of named parameters</param>
		/// <returns></returns>
		public static IEnumerable<T> Query<T>(CommandType type, string sql, int timeout, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(defaultConnection, type, sql, timeout, parameters);
		}

		/// <summary>
		/// Executes the SQL using the specified connection string and returns an IEnumerable of <typeparamref name="T"/>
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="type"><typeparamref name="System.Data.CommandType"/></param>
		/// <param name="connection">Name of connection string specified in the Connection Strings section of app.settings/web.config</param>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="parameters">>Optional list of named parameters</param>
		/// <returns></returns>
		public static IEnumerable<T> Query<T>(CommandType type, string connection, string sql, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(connection, type, sql, 0, parameters);
		}

		/// <summary>
		/// Executes the SQL using the specified connection string and returns an IEnumerable of <typeparamref name="T"/>
		/// </summary>
		/// <typeparam name="T">POCO class with at least an empty constuctor</typeparam>
		/// <param name="type"><typeparamref name="System.Data.CommandType"/></param>
		/// <param name="connection">Name of connection string specified in the Connection Strings section of app.settings/web.config</param>
		/// <param name="sql">SQL Statement to execute</param>
		/// <param name="timeout">Timeout value for executing then command</param>
		/// <param name="parameters">>Optional list of named parameters</param>
		/// <returns></returns>
		public static IEnumerable<T> Query<T>(CommandType type, string connection, string sql, int timeout, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			return QueryInternal<T>(connection, type, sql, timeout, parameters);
		}
		#endregion


	








		private static IEnumerable<T> QueryInternal<T>(string connection, CommandType type, string sql, int timeout, params System.Data.IDbDataParameter[] parameters) where T : new()
		{
			using (var db = GetConnection(connection))
			{
				db.Open();

				using (var dc = db.CreateCommand())
				{
					dc.Connection = db;
					dc.CommandText = sql;
					dc.CommandType = type;

					if (timeout > 0) { dc.CommandTimeout = timeout; }

					foreach (IDbDataParameter param in parameters) { dc.Parameters.Add(param); }

					using (var reader = dc.ExecuteReader(CommandBehavior.CloseConnection))
					{
						var deserialiser = GetDeserliser<T>(typeof(T).ToString() + '-' + dc.CommandText + '-' + db.ConnectionString, reader, 0, -1);

						while (reader.Read())
						{
							yield return deserialiser(reader);
						}
					}

				}

			}

		}


		public static IEnumerable<TRet> QueryMapped<T1, T2, TRet>(Func<T1, T2, TRet> cb, string sql)
			where T1 : new()
			where T2 : new()
			where TRet : new()
		{
			return QueryMapped<T1, T2, TRet>(cb, sql, "Id");
		}
		public static IEnumerable<TRet> QueryMapped<T1, T2, TRet>(Func<T1, T2, TRet> cb, string sql, string splitOn)
			where T1 : new()
			where T2 : new()
			where TRet : new()
		{

			if (splitOn == null || splitOn.Trim().Length == 0)
			{
				splitOn = "Id";
			}

			using (var db = GetConnection(defaultConnection))
			{
				db.Open();

				using (var dc = db.CreateCommand())
				{
					dc.Connection = db;
					dc.CommandText = sql;
					dc.CommandType = CommandType.Text;

					//if (timeout > 0) { dc.CommandTimeout = timeout; }

					//foreach (IDbDataParameter param in parameters) { dc.Parameters.Add(param); }

					using (var reader = dc.ExecuteReader(CommandBehavior.CloseConnection))
					{

						// from Dapper **************************************************************************************

						int current = 0;

						var splits = splitOn.Split(',').ToArray();
						var splitIndex = 0;

						Func<Type, int> nextSplit = type =>
						{
							var currentSplit = splits[splitIndex];
							if (splits.Length > splitIndex + 1)
							{
								splitIndex++;
							}

							bool skipFirst = false;
							int startingPos = current + 1;
							// if our current type has the split, skip the first time you see it. 
							if (type != typeof(Object))
							{
								var props = GetSettableProps(type);
								var fields = GetSettableFields(type);

								foreach (var name in props.Select(p => p.Name).Concat(fields.Select(f => f.Name)))
								{
									if (string.Equals(name, currentSplit, StringComparison.OrdinalIgnoreCase))
									{
										skipFirst = true;
										startingPos = current;
										break;
									}
								}

							}


							int pos;
							for (pos = startingPos; pos < reader.FieldCount; pos++)
							{
								// some people like ID some id ... assuming case insensitive splits for now
								if (splitOn == "*")
								{
									break;
								}
								if (string.Equals(reader.GetName(pos), currentSplit, StringComparison.OrdinalIgnoreCase))
								{
									if (skipFirst)
									{
										skipFirst = false;
									}
									else
									{
										break;
									}
								}
							}
							current = pos;
							return pos;
						};
						// **************************************************************************************************

						int split = nextSplit(typeof(T1));
						var deserialiser1 = GetDeserliser<T1>(typeof(T1).ToString() + '-' + dc.CommandText + '-' + db.ConnectionString, reader, 0, split);

						int next = nextSplit(typeof(T2));
						var deserialiser2 = GetDeserliser<T2>(typeof(T2).ToString() + '-' + dc.CommandText + '-' + db.ConnectionString, reader, split, next - split);
						split = next;

						bool needTerminator = false;

						while (reader.Read())
						{
							var v1 = deserialiser1(reader);
							var v2 = deserialiser2(reader);
							var vr = cb(v1, v2);
							if (vr != null)
								yield return vr;
							else
								needTerminator = true;
						}

						if (needTerminator == true)
						{
							var vr = cb(default(T1), default(T2));
							if (vr != null)
								yield return vr;
						}

					}

				}

			}

		}
			





		public static IEnumerable<T> Query<T>(this IDataReader reader) where T : new()
		{
			var deserialiser = GetDeserliser<T>(reader, 0, -1);

			while (reader.Read())
			{
				yield return deserialiser(reader);
			}

			reader.NextResult();
		}


		private static string defaultConnection = "sqlConnection";
		private static IDbConnection GetConnection(string connection)
		{
			//todo: using connectionStrings provider name, create appropriate connection...
			return new SqlConnection(ConnectionString(connection));
		}
		private static string ConnectionString(string ConnectionID)
		{
			return ConfigurationManager.ConnectionStrings[ConnectionID].ConnectionString;
		}


		#region "Custom Object Deserliser(IL) Generation"
		static MethodInfo fnIsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");
		static MethodInfo fnGetValue = typeof(IDataRecord).GetMethod("GetValue", new Type[] { typeof(int) });
		static MethodInfo fnGetInt32 = typeof(IDataRecord).GetMethod("GetInt32", new Type[] { typeof(int) });
		static MethodInfo fnGetString = typeof(IDataRecord).GetMethod("GetString", new Type[] { typeof(int) });
		static MethodInfo fnGetGuid = typeof(IDataRecord).GetMethod("GetGuid", new Type[] { typeof(int) });
		static MethodInfo fnGetDecimal = typeof(IDataRecord).GetMethod("GetDecimal", new Type[] { typeof(int) });
		static MethodInfo fnGetDateTime = typeof(IDataRecord).GetMethod("GetDateTime", new Type[] { typeof(int) });


		//todo: implement cache with LRU removal?
		// cache of dynamically generated deserlisation functions.
		static Dictionary<string, object> pocoFactories = new Dictionary<string, object>();

		private static Func<IDataReader, T> GetDeserliser<T>(IDataReader reader, int startBound, int length) where T : new()
		{
			Type iType = typeof(T);

			// simple type?
			if (iType.IsValueType || iType == typeof(string) || iType == typeof(byte[]))
			{
				return (rdr) => (T)rdr.GetValue(0);
			}
			else
			{

				if (length == -1)
				{
					length = reader.FieldCount - startBound;
				}
				if (reader.FieldCount <= startBound)
				{
					throw new ArgumentException("When using the multi-mapping APIs ensure you set the splitOn param if you have keys other than Id", "splitOn");
				}


				var dm = new DynamicMethod(string.Format("Deserialize{0}", Guid.NewGuid()), iType, new[] { typeof(IDataReader) }, true);
				var il = dm.GetILGenerator();

				// get Properties and Fields of T that we should be able to set
				PropertyInfo[] cachedProps = iType.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
				FieldInfo[] cachedFields = iType.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

				PropertyInfo currentProp;
				FieldInfo currentField = null;

				// <T> item;
				il.DeclareLocal(iType);

				// item = new <T>();
				il.Emit(OpCodes.Newobj, iType.GetConstructor(Type.EmptyTypes));								// <T>
				il.Emit(OpCodes.Stloc_0);																	//

				for (int i = startBound; i < startBound + length; i++)
				{
					bool unboxed = false;
					string pName = reader.GetName(i);
					currentProp = cachedProps.SingleOrDefault(x => x.Name.Equals(pName, StringComparison.InvariantCultureIgnoreCase));

					//if the property is null, likely it's a Field
					if (currentProp == null)
						currentField = cachedFields.SingleOrDefault(x => x.Name.Equals(pName, StringComparison.InvariantCultureIgnoreCase));




					// we have either a property or field that we can assign to....
					if (currentProp != null || currentField != null)
					{

						// if(!reader.IsDBNull(i))
						il.Emit(OpCodes.Ldarg_0);																// reader
						EmitInt32(il, i); // il.Emit(OpCodes.Ldc_I4, i);										// reader, i
						il.Emit(OpCodes.Callvirt, fnIsDBNull);													// [bool]
						var lblNext = il.DefineLabel();
						il.Emit(OpCodes.Brtrue_S, lblNext);														//
						//{

						// "<T>.property/field = (type) reader.GetValue(i);"

						// reader.GetValue(i);
						il.Emit(OpCodes.Ldloc_0);																// <T>
						il.Emit(OpCodes.Ldarg_0);																// <T>, reader 
						EmitInt32(il, i); //il.Emit(OpCodes.Ldc_I4, i);											// <T>, reader, i


						// need to refactor this block to better handle reader.GetXXXX(i) calls and assigning/casting to <T>.Fields/Properties.
						// ie 
						//		db datatype = int32 and <T>.Field/Property = int32 then assign reader.GetInt32(i)
						//		db datatype = guid and <T>.Field/Property = guid then assign reader.GetGuid(i)
						//		db datatype = string and <T>.Field/Property = string then assign reader.GetString(i)
						//		etc.....
						//		also boolean
						//			ie database value ="1" or "True" then <T>.Field/Property = True
						//	Nullable <T>.Field/Properties assign null?

						var tt = reader.GetFieldType(i);
						if (tt == typeof(Int32))
						{
							il.Emit(OpCodes.Callvirt, fnGetInt32);												// <T>, [value(Int32)]
							unboxed = true;
						}
						//else if (tt == typeof(String))
						//{
						//    il.Emit(OpCodes.Callvirt, fnGetString);											// <T>, [value(String)]
						//    unboxed = true;
						//}
						else if (tt == typeof(Guid))
						{
							il.Emit(OpCodes.Callvirt, fnGetGuid);												// <T>, [value(Guid)]
							unboxed = true;
						}
						else if (tt == typeof(Decimal))
						{
							il.Emit(OpCodes.Callvirt, fnGetDecimal);											// <T>, [value(Decimal)]
							unboxed = true;
						}
						else if (tt == typeof(DateTime))
						{
							il.Emit(OpCodes.Callvirt, fnGetDateTime);											// <T>, [value(DateTime)]
							unboxed = true;
						}
						else
						{
							il.Emit(OpCodes.Callvirt, fnGetValue);												// <T>, [value]
						}
						//reader.GetBoolean(i);


						if (currentProp != null)
						{
							if (!unboxed)
							{
								if (currentProp.PropertyType == typeof(Uri))
								{
									il.Emit(OpCodes.Unbox_Any, typeof(string));
									il.Emit(OpCodes.Newobj, typeof(Uri).GetConstructor(new[] { typeof(string) }));	// <T>, [Uri]

									unboxed = true;

								}
								else if (currentProp.PropertyType == typeof(Guid) && reader.GetFieldType(i) != typeof(Guid))
								{
									il.Emit(OpCodes.Unbox_Any, typeof(string));
									il.Emit(OpCodes.Newobj, typeof(Guid).GetConstructor(new[] { typeof(string) }));	// <T>, [Guid]

									unboxed = true;
								}
							}

							//<T>.[property] = (type) [value];
							if (!unboxed)
							{
								il.Emit(OpCodes.Unbox_Any, currentProp.PropertyType);							// <T>, [value]
							}

							// call set even if it is private
							il.Emit(OpCodes.Callvirt, currentProp.GetSetMethod(true));							//
						}
						else
						{
							if (!unboxed)
							{
								if (currentField.FieldType == typeof(Uri))
								{
									il.Emit(OpCodes.Unbox_Any, typeof(string));
									il.Emit(OpCodes.Newobj, typeof(Uri).GetConstructor(new[] { typeof(string) }));	// <T>, [Uri]

									unboxed = true;

								}
								else if (currentField.FieldType == typeof(Guid) && reader.GetFieldType(i) != typeof(Guid))
								{
									il.Emit(OpCodes.Unbox_Any, typeof(string));
									il.Emit(OpCodes.Newobj, typeof(Guid).GetConstructor(new[] { typeof(string) }));	// <T>, [Guid]

									unboxed = true;
								}
							}
							//<T>.[field] = (type) [value];
							if (!unboxed)
							{
								il.Emit(OpCodes.Unbox_Any, currentField.FieldType);								// <T>, [value]
							}
							il.Emit(OpCodes.Stfld, currentField);												//
						}
						//} 
						il.MarkLabel(lblNext);
					}
				}

				il.Emit(OpCodes.Ldloc_0);
				il.Emit(OpCodes.Ret);


				var factory = (Func<IDataReader, T>)dm.CreateDelegate(typeof(Func<IDataReader, T>));
				return factory as Func<IDataReader, T>;
			}
		}

		private static Func<IDataReader, T> GetDeserliser<T>(string key, IDataReader reader, int startBound, int length) where T : new()
		{
			// look up our cache.
			lock (pocoFactories)
			{
				object factory;
				if (pocoFactories.TryGetValue(key, out factory))
				{
					return factory as Func<IDataReader, T>;
				}
			}


			var factory2 = GetDeserliser<T>(reader, startBound, length);

			lock (pocoFactories)
			{
				pocoFactories[key] = factory2;
			}

			SaveAssembly<T>(reader, startBound, length);

			return factory2 as Func<IDataReader, T>;

		}



		// from Dapper **************************************************************************************
		private static void EmitInt32(ILGenerator il, int value)
		{
			switch (value)
			{
				case -1: il.Emit(OpCodes.Ldc_I4_M1); break;
				case 0: il.Emit(OpCodes.Ldc_I4_0); break;
				case 1: il.Emit(OpCodes.Ldc_I4_1); break;
				case 2: il.Emit(OpCodes.Ldc_I4_2); break;
				case 3: il.Emit(OpCodes.Ldc_I4_3); break;
				case 4: il.Emit(OpCodes.Ldc_I4_4); break;
				case 5: il.Emit(OpCodes.Ldc_I4_5); break;
				case 6: il.Emit(OpCodes.Ldc_I4_6); break;
				case 7: il.Emit(OpCodes.Ldc_I4_7); break;
				case 8: il.Emit(OpCodes.Ldc_I4_8); break;
				default: il.Emit(OpCodes.Ldc_I4, value); break;
			}
		}

		class PropInfo
		{
			public string Name { get; set; }
			public MethodInfo Setter { get; set; }
			public Type Type { get; set; }
		}

		static List<PropInfo> GetSettableProps(Type t)
		{
			return t
				  .GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
				  .Select(p => new PropInfo
				  {
					  Name = p.Name,
					  Setter = p.DeclaringType == t ? p.GetSetMethod(true) : p.DeclaringType.GetProperty(p.Name).GetSetMethod(true),
					  Type = p.PropertyType
				  })
				  .Where(info => info.Setter != null)
				  .ToList();
		}

		static List<FieldInfo> GetSettableFields(Type t)
		{
			return t.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).ToList();
		}

		// **************************************************************************************************



		//http://msdn.microsoft.com/en-us/library/8zwdfdeh.aspx
		private static void SaveAssembly<T>(IDataReader reader, int startBound, int length)
		{

			AssemblyName assemblyName = new AssemblyName();
			assemblyName.Name = "myDynamicAssembly";

			AssemblyBuilder assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndSave);


			ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule("MyDynamicModule.dll", "MyDynamicModule.dll");

			TypeBuilder typeBuilder = moduleBuilder.DefineType("MyDynamicType");

			MethodBuilder mb = typeBuilder.DefineMethod("DeseriliseMethodDynamic", MethodAttributes.Public, typeof(T), new[] { typeof(IDataReader) });
			ILGenerator il = mb.GetILGenerator();


			// get Public Properties and fields of T
			Type iType = typeof(T);

			PropertyInfo[] cachedProps = iType.GetProperties();
			FieldInfo[] cachedFields = iType.GetFields();

			PropertyInfo currentProp;
			FieldInfo currentField = null;


			//var item = new T();
			il.DeclareLocal(iType);
			il.Emit(OpCodes.Newobj, iType.GetConstructor(Type.EmptyTypes));								// <T>
			il.Emit(OpCodes.Stloc_0);																	//

			for (int i = 0; i < reader.FieldCount; i++)
			{
				bool unboxed = false;
				string pName = reader.GetName(i);
				currentProp = cachedProps.SingleOrDefault(x => x.Name.Equals(pName, StringComparison.InvariantCultureIgnoreCase));

				//if the property is null, likely it's a Field
				if (currentProp == null)
					currentField = cachedFields.SingleOrDefault(x => x.Name.Equals(pName, StringComparison.InvariantCultureIgnoreCase));

				// we have either a property or field that we can assign to....
				if (currentProp != null || currentField != null)
				{

					// if(!reader.IsDBNull(i))
					il.Emit(OpCodes.Ldarg_0);															// reader
					EmitInt32(il, i); // il.Emit(OpCodes.Ldc_I4, i);									// reader, i
					il.Emit(OpCodes.Callvirt, fnIsDBNull);												// [bool]
					var lblNext = il.DefineLabel();
					il.Emit(OpCodes.Brtrue_S, lblNext);													//
					//{

					// "<T>.property/field = (type) reader.GetValue(i);"
					// reader.GetValue(i);
					il.Emit(OpCodes.Ldloc_0);															// <T>
					il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
					EmitInt32(il, i); //il.Emit(OpCodes.Ldc_I4, i);										// <T>, reader, i
					il.Emit(OpCodes.Callvirt, fnGetValue);												// <T>, [value]


					//<T>/[field] = (type) [value];
					if ((currentProp != null && currentProp.PropertyType == typeof(Uri)) || (currentField != null && currentField.FieldType == typeof(Uri)))
					{
						il.Emit(OpCodes.Unbox_Any, typeof(string));
						il.Emit(OpCodes.Newobj, typeof(Uri).GetConstructor(new[] { typeof(string) }));	// <T>, [Uri]

						unboxed = true;
					}
					else if ((currentProp != null && currentProp.PropertyType == typeof(Guid)) || (currentField != null && currentField.FieldType == typeof(Guid)))
					{
						if (reader.GetFieldType(i) != typeof(Guid))
						{
							il.Emit(OpCodes.Unbox_Any, typeof(string));
							il.Emit(OpCodes.Newobj, typeof(Guid).GetConstructor(new[] { typeof(string) }));	// <T>, [Guid]

							unboxed = true;
						}
					}




					if (currentProp != null)
					{
						//<T>.[property] = (type) [value];
						if (!unboxed)
						{
							il.Emit(OpCodes.Unbox_Any, currentProp.PropertyType);						// <T>, [value]
						}

						// call set even if it is private
						il.Emit(OpCodes.Callvirt, currentProp.GetSetMethod(true));						//
					}
					else
					{
						//<T>.[field] = (type) [value];
						if (!unboxed)
						{
							il.Emit(OpCodes.Unbox_Any, currentField.FieldType);							// <T>, [value]
						}
						il.Emit(OpCodes.Stfld, currentField);											//
					}
					//} 
					il.MarkLabel(lblNext);
				}
			}

			il.Emit(OpCodes.Ldloc_0);
			il.Emit(OpCodes.Ret);


			var t = typeBuilder.CreateType();

			if (System.IO.File.Exists("MyDynamicModule.dll")) { System.IO.File.Delete("MyDynamicModule.dll"); }
			assemblyBuilder.Save("MyDynamicModule.dll");

		}


		#endregion
	}
}