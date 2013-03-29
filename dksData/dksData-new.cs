using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace dksData
{
	public static partial class Database
	{

		// Query
		// IL Generation


		// do we want to cache for information about the method? sql, use count, cache time, last used?
		private static ConcurrentDictionary<string, object> pocoFactories = new ConcurrentDictionary<string, object>();


		// Query<TRet>(this IDbConnection db, string sql, params object[] parameters)
		// Query<TRet>(this IDbConnection db, Type[] types, object callback, string sql, params object[] parameters)

		public static IEnumerable<TRet> Query<TRet>(this IDbConnection db, string sql, params object[] parameters)
		{

			using (var cmd = CreateCommand(db, sql, parameters))
			{

				using (var reader = cmd.ExecuteReader())
				{

					//Func<IDataReader, TRet> deserialiser = null;
					Func<IDataReader, object> deserialiser = null;

					// this assumes query will allways return same query and not change result sets depending on parameters and there values...
					string cacheKey;
					cacheKey = GetCacheKey(typeof(TRet), db, cmd, sql);

					// look up cache
					lock (pocoFactories)
					{
						object factory;
						if (pocoFactories.TryGetValue(cacheKey, out factory))
						{
							//deserialiser = factory as Func<IDataReader, TRet>;
							deserialiser = factory as Func<IDataReader, object>;

						}
					}

					if (deserialiser == null)
					{
						// make function
						deserialiser = GetDeserliser<TRet>(reader);

						// cache it
						lock (pocoFactories)
						{
							pocoFactories[cacheKey] = deserialiser;
						}
					}


					while (reader.Read())
					{
						yield return (TRet)deserialiser(reader);
					}

					reader.Close();
				}

			}

		}

		private static string GetCacheKey(Type type, IDbConnection db, IDbCommand dc, string sql)
		{
			return type.ToString() + '-' + dc.CommandText + '-' + db.ConnectionString;
		}



		//private static Func<IDataReader, TRet> GetDeserliser<TRet>(IDataReader reader)
		private static Func<IDataReader, object> GetDeserliser<TRet>(IDataReader reader)
		{

			Type returnType = typeof(TRet);

			if (
				(returnType.IsClass && returnType != typeof(string) && returnType != typeof(byte[]))
				|| (returnType.IsValueType && !returnType.IsPrimitive && returnType != typeof(DateTime) && !(returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(Nullable<>))))
			{
				// define our custom method 
				// static TRet DeserialiseXXXXX(reader) {}
				//var dm = new DynamicMethod(string.Format("Deserialise{0}", Guid.NewGuid()), returnType, new[] { typeof(IDataReader) }, true);
				var dm = new DynamicMethod(string.Format("Deserialise{0}", Guid.NewGuid()), typeof(object), new[] { typeof(IDataReader) }, true);

				var il = dm.GetILGenerator();

				// int idx;     // so we know the column index into reader that caused us grief.
				LocalBuilder idx = il.DeclareLocal(typeof(int));


				// define local var for return type.
				LocalBuilder returnItem = il.DeclareLocal(returnType);


				//try {
				il.BeginExceptionBlock();

				// method section to build returnItem
				il.GenerateMethodBlock(reader, idx, 0, -1, returnItem);


				//} catch (Exception ex) {
				il.BeginCatchBlock(typeof(Exception));												// ex

				// db.ThrowDataException(Exception ex, int idx, IDataReader reader);
				il.Emit(OpCodes.Ldloc, idx);														// ex, idx
				il.Emit(OpCodes.Ldarg_0);                       									// ex, idx, reader

				il.EmitCall(OpCodes.Call, MethodInfo.GetCurrentMethod().DeclaringType.GetMethod("ThrowDataException", BindingFlags.Static | BindingFlags.NonPublic), null);

				if (!returnType.IsValueType)
				{
					// item = null;
					il.Emit(OpCodes.Ldnull);														// ex, null
					il.Emit(OpCodes.Stloc, returnItem);												// ex
				}

				//}
				il.EndExceptionBlock();


				// return item;
				il.Emit(OpCodes.Ldloc, returnItem);
				if (returnType.IsValueType)
				{
					il.Emit(OpCodes.Box, returnType);
				}
				il.Emit(OpCodes.Ret);

				//} end of method


				// finish building/compile function.
				//var factory = (Func<IDataReader, TRet>)dm.CreateDelegate(typeof(Func<IDataReader, TRet>));
				var factory = (Func<IDataReader, object>)dm.CreateDelegate(typeof(Func<IDataReader, object>));

				return factory;
			}
			else
			{
				// todo: Is there a smarter/better way?
				return (rdr) => (TRet)Convert.ChangeType(rdr.GetValue(0), returnType);
			}

		}

		private static void GenerateMethodBlock(this ILGenerator il, IDataReader reader, LocalBuilder idx, int startBound, int length, LocalBuilder returnItem)
		{
			Type srcType;
			Type dstType;
			Type nullUnderlyingType;
			MethodInfo fnGetMethod;
			Label lblNext;




			if (length == -1)
			{
				length = reader.FieldCount - startBound;
			}
			if (reader.FieldCount <= startBound)
			{
				//todo: fix error message
				throw new ArgumentException("When using the multi-mapping APIs ensure you set the splitOn param if you have keys other than Id", "splitOn");
			}

			Type returnType;
			returnType = returnItem.LocalType;


			// get Properties and Fields of T that we should be able to set
			var properties = GetSettableProperties(returnType);
			var fields = GetSettableFields(returnType);


			if (returnType.IsValueType)
			{
				// structs
				il.Emit(OpCodes.Ldloca, returnItem);
				il.Emit(OpCodes.Initobj, returnType);
			}
			else
			{
				// objects
				// item = new <T>();    // using public or private constructor.
				il.Emit(OpCodes.Newobj, returnType.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, Type.EmptyTypes, null));	// <T>
				il.Emit(OpCodes.Stloc, returnItem);										                                                                                //
			}

			for (int i = startBound; i < startBound + length; i++)
			{
				// get type for reader column(i)
				// is there a matching property/field in item
				//  assign with appropriate casting/boxing etc...

				// ***************************************************************************************************************************************

				// select matching property or field, ordering by properties (case sensitive, case insensitive) then fields (case sensitive, case insensitive)
				Setter ps;
				ps = new Setter
				{
					prop = properties.FirstOrDefault(p => string.Equals(p.Name, reader.GetName(i), StringComparison.InvariantCulture)) ?? properties.FirstOrDefault(p => string.Equals(p.Name, reader.GetName(i), StringComparison.InvariantCultureIgnoreCase)),
					field = fields.FirstOrDefault(f => string.Equals(f.Name, reader.GetName(i), StringComparison.InvariantCulture)) ?? fields.FirstOrDefault(f => string.Equals(f.Name, reader.GetName(i), StringComparison.InvariantCultureIgnoreCase))
				};


				// did we find a matching property / field?
				if (ps.prop == null && ps.field == null)
				{
					continue;
				}


				srcType = reader.GetFieldType(i);
				dstType = ps.prop != null ? ps.prop.PropertyType : ps.field.PropertyType;
				nullUnderlyingType = Nullable.GetUnderlyingType(dstType);


				// if(!reader.IsDBNull(i))																	// [Stack]
				// {
				il.Emit(OpCodes.Ldarg_0);																	// reader
				il.EmitFastInt(i);																			// reader, i

				// idx = i;
				il.Emit(OpCodes.Dup);																		// reader, i, i
				il.Emit(OpCodes.Stloc, idx);																// reader, i

				il.Emit(OpCodes.Callvirt, Functions.IsDBNull);												// [bool]
				lblNext = il.DefineLabel();
				il.Emit(OpCodes.Brtrue_S, lblNext);															//


				// "<T>.property/field = (type) reader.GetValue(i);"

				// Is there a direct GetXXX(i) method?
				fnGetMethod = typeof(IDataRecord).GetMethod("Get" + srcType.Name, new Type[] { typeof(int) });

				if (dstType.IsAssignableFrom(srcType))
				{
					if (returnType.IsValueType)
					{
						il.Emit(OpCodes.Ldloca, returnItem);												// <T>
					}
					else
					{
						il.Emit(OpCodes.Ldloc, returnItem);													// <T>
					}
					il.Emit(OpCodes.Ldarg_0);																// <T>, reader 
					il.EmitFastInt(i);																		// <T>, reader, i

					if (fnGetMethod != null)
					{
						il.Emit(OpCodes.Callvirt, fnGetMethod);												// <T>, [value as type]
					}
					else
					{
						// getValue, unbox
						il.Emit(OpCodes.Callvirt, Functions.GetValue);										// <T>, [value as object]
						il.Emit(OpCodes.Unbox_Any, dstType);												// <T>, [value as type]
					}

					if (nullUnderlyingType != null)
					{
						il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));
					}

					// assign to <T>.Field/Property
					il.EmitMemberAssignment(returnType, ps);												// <T>

				}
				else
				{
					// Not directly assignable, we'll do some common custom mapping before falling back to calling Convert.ChangeType(..)

					// String/Int => Enum
					if (dstType.IsEnum || (nullUnderlyingType != null && nullUnderlyingType.IsEnum))
					{
						if (IsNumericType(srcType))
						{
							if (returnType.IsValueType)
							{
								il.Emit(OpCodes.Ldloca, returnItem);										// <T>
							}
							else
							{
								il.Emit(OpCodes.Ldloc, returnItem);											// <T>
							}
							il.Emit(OpCodes.Ldarg_0);														// <T>, reader 
							il.EmitFastInt(i);																// <T>, reader, i

							il.Emit(OpCodes.Callvirt, Functions.GetValue);									// <T>, [value as object]
							il.Emit(OpCodes.Unbox_Any, typeof(int));										// <T>, [value as type]

							if (nullUnderlyingType != null)
							{
								il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));
							}

							// assign to <T>.Field/Property
							il.EmitMemberAssignment(returnType, ps);										// <T>

						}
						else if (srcType == typeof(string))
						{
							if (returnType.IsValueType)
							{
								il.Emit(OpCodes.Ldloca, returnItem);										// <T>
							}
							else
							{
								il.Emit(OpCodes.Ldloc, returnItem);											// <T>
							}

							if (nullUnderlyingType != null)
							{
								il.Emit(OpCodes.Ldtoken, nullUnderlyingType);								// <T>, token
							}
							else
							{
								il.Emit(OpCodes.Ldtoken, dstType);											// <T>, token
							}
							il.EmitCall(OpCodes.Call, Functions.GetTypeFromHandle, null);					// <T>, dstType/nullUnderlyingType

							il.Emit(OpCodes.Ldarg_0);														// <T>, dstType/nullUnderlyingType, reader 
							il.EmitFastInt(i);																// <T>, dstType/nullUnderlyingType, reader, i
							il.Emit(OpCodes.Callvirt, Functions.GetString);									// <T>, dstType/nullUnderlyingType, [value as String]
							il.EmitFastInt(1);																// <T>, dstType/nullUnderlyingType, [value as String], true

							il.EmitCall(OpCodes.Call, Functions.EnumParse, null);							// <T>, enum
							il.Emit(OpCodes.Unbox_Any, dstType);											// <T>, [enum as dstType]

							// assign to <T>.Field/Property
							il.EmitMemberAssignment(returnType, ps);										// <T>

						}
					}
					else if (dstType == typeof(Uri) && srcType == typeof(string))
					{
						// String => Uri

						if (returnType.IsValueType)
						{
							il.Emit(OpCodes.Ldloca, returnItem);											// <T>
						}
						else
						{
							il.Emit(OpCodes.Ldloc, returnItem);												// <T>
						}

						il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
						il.EmitFastInt(i);																	// <T>, reader, i

						il.Emit(OpCodes.Callvirt, Functions.GetString);										// <T>, [string]
						il.Emit(OpCodes.Newobj, typeof(Uri).GetConstructor(new[] { typeof(string) }));		// <T>, [Uri]

						// assign to <T>.Field/Property
						il.EmitMemberAssignment(returnType, ps);											// <T>

					}
					else if ((dstType == typeof(Guid) || nullUnderlyingType == typeof(Guid)) && srcType == typeof(string))
					{
						// String => Guid

						if (returnType.IsValueType)
						{
							il.Emit(OpCodes.Ldloca, returnItem);											// <T>
						}
						else
						{
							il.Emit(OpCodes.Ldloc, returnItem);												// <T>
						}

						il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
						il.EmitFastInt(i);																	// <T>, reader, i

						il.Emit(OpCodes.Callvirt, Functions.GetString);										// <T>, [value as string]
						il.EmitCall(OpCodes.Call, Functions.GuidParse, null);								// <T>, guid

						if (nullUnderlyingType != null)
						{
							il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));
						}

						// assign to <T>.Field/Property
						il.EmitMemberAssignment(returnType, ps);											// <T>
					}
					else
					{

						// o = reader.GetValue(i);
						if (returnType.IsValueType)
						{
							il.Emit(OpCodes.Ldloca, returnItem);											// <T>
						}
						else
						{
							il.Emit(OpCodes.Ldloc, returnItem);												// <T>
						}
						il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
						il.EmitFastInt(i);																	// <T>, reader, i
						il.Emit(OpCodes.Callvirt, Functions.GetValue);										// <T>, [value as object]

						//  = (dstType) Convert.ChangeType(o, typeof(dstType/nullUnderlyingType);
						if (nullUnderlyingType != null)
						{
							il.Emit(OpCodes.Ldtoken, nullUnderlyingType);
							il.EmitCall(OpCodes.Call, Functions.GetTypeFromHandle, null);					// <T>, value, type(nullUnderlyingType)

							il.Emit(OpCodes.Call, Functions.ConvertChangeType);								// <T>, [value as object of nullUnderlyingType]	
							il.Emit(OpCodes.Unbox_Any, nullUnderlyingType);									// <T>, [value as nullUnderlyingType)

							il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));	// <T>, [value as dstType]

						}
						else
						{
							il.Emit(OpCodes.Ldtoken, dstType);
							il.EmitCall(OpCodes.Call, Functions.GetTypeFromHandle, null);					// <T>, value, type(dstType)

							il.Emit(OpCodes.Call, Functions.ConvertChangeType);								// <T>, [value as object of dstType]	
							il.Emit(OpCodes.Unbox_Any, dstType);											// <T>, [value as dstType)

						}

						// assign to <T>.Field/Property
						il.EmitMemberAssignment(returnType, ps);											// <T>

					}
				}
				//}
				il.MarkLabel(lblNext);	// end of if(!reader.IsDBNull(i))

				// ***************************************************************************************************************************************
			}


		}


		public struct Setter
		{
			public settableProperty prop;
			public settableField field;
		}




		// todo: ThrowNiceDataException(Exception ex, IDataReader reader, int index, Type currentType, string currentField/Property) {}
		private static void ThrowDataException(Exception ex, int index, IDataReader reader)
		{
			// an exception was thrown/caught in our custom IL deseralise method. re throw with some nice detail.

			string name = "(n/a)", value = "(n/a)";
			if (reader != null && index >= 0 && index < reader.FieldCount)
			{
				name = reader.GetName(index);
				object val = reader.GetValue(index);
				if (val == null || val is DBNull)
				{
					value = "(null)[" + Type.GetTypeCode(val.GetType()) + "]";
				}
				else
				{
					value = Convert.ToString(val) + "[" + Type.GetTypeCode(val.GetType()) + "]";
				}
			}

			throw new DataException(string.Format("Error assigning value {0} to {1}", value, name), ex);

		}

		private static bool IsNumericType(Type type)
		{
			if (type == null)
			{
				return false;
			}

			switch (Type.GetTypeCode(type))
			{
				case TypeCode.SByte:
				case TypeCode.Byte:
				case TypeCode.Int16:
				case TypeCode.UInt16:
				case TypeCode.Int32:
				case TypeCode.UInt32:
				case TypeCode.Int64:
				case TypeCode.UInt64:
				case TypeCode.Single:
				case TypeCode.Double:
				case TypeCode.Decimal:
					return true;
				case TypeCode.Object:
					if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
					{
						return IsNumericType(Nullable.GetUnderlyingType(type));
					}
					return false;
			}
			return false;
		}


	}
}
