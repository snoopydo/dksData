using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;

namespace dksData
{
	public static partial class Database
	{

		// include dksData.GetConnection.cs
		// include dksData.GetCommand.cs
		// include dksData.ExecuteXXX.cs


		// Query
		// IL Generation




		#region Query(...)

		private static Dictionary<string, object> pocoFactories = new Dictionary<string, object>();


		// Query<TRet>(this IDbConnection db, string sql, params object[] parameters)
		// Query<TRet>(this IDbConnection db, Type[] types, object callback, string sql, params object[] parameters)

		public static IEnumerable<TRet> Query<TRet>(this IDbConnection db, string sql, params object[] parameters)
		{

			using (var cmd = CreateCommand(db, sql, parameters))
			{

				using (var reader = cmd.ExecuteReader())
				{

					Func<IDataReader, TRet> deserialiser = null;

					// this assumes query will allways return same query and not change result sets depending on parameters and there values...
					string cacheKey;
					cacheKey = GetCacheKey(typeof(TRet), db, cmd, sql);

					// look up cache
					lock (pocoFactories)
					{
						object factory;
						if (pocoFactories.TryGetValue(cacheKey, out factory))
						{
							deserialiser = factory as Func<IDataReader, TRet>;
						}
					}

					if (deserialiser == null)
					{
						// make function
						deserialiser = GetDeserliser2<TRet>(reader);

						// cache it
						lock (pocoFactories)
						{
							pocoFactories[cacheKey] = deserialiser;
						}
					}


					while (reader.Read())
					{
						yield return deserialiser(reader);
					}

					reader.Close();
				}

			}

		}

		private static string GetCacheKey(Type type, IDbConnection db, IDbCommand dc, string sql)
		{
			return type.ToString() + '-' + dc.CommandText + '-' + db.ConnectionString;
		}

		#endregion


		private static Func<IDataReader, TRet> GetDeserliser2<TRet>(IDataReader reader)
		{

			// for now, assume TRet is a class, we need to handle ints, string, structs etc...

			Type type = typeof(TRet);

			// tempary, need to implement proper handling of classes, sturcts, etc...
			if (type.IsValueType || type == typeof(string) || type == typeof(byte[]))
			{
				return (rdr) => (TRet)rdr.GetValue(0);
			}
			else if (IsStructure(type))
			{
				// is rubbish, need to build method similar to that for handling classes below.
				return (rdr) => (TRet)rdr.GetValue(0);
			}
			else
			{

				// define our custom method 
				// static TRet DeserialiseXXXXX(reader) {}
				var dm = new DynamicMethod(string.Format("Deserialise{0}", Guid.NewGuid()), type, new[] { typeof(IDataReader) }, true);
				var il = dm.GetILGenerator();

				// define local var for return type.
				LocalBuilder returnItem = il.DeclareLocal(type);

				il.GenerateMethodBlock(reader, 0, -1, returnItem);

				// return item;
				il.Emit(OpCodes.Ldloc, returnItem);
				il.Emit(OpCodes.Ret);


				// finish building/compile function.
				var factory = (Func<IDataReader, TRet>)dm.CreateDelegate(typeof(Func<IDataReader, TRet>));

				return factory;
			}
		}

		private static void GenerateMethodBlock(this ILGenerator il, IDataReader reader, int startBound, int length, LocalBuilder item)
		{
			Type srcType;
			Type dstType;
			Type nullUnderlyingType;
			bool isAssignable;
			MethodInfo fnGetMethod;
			Label lblNext;




			if (length == -1)
			{
				length = reader.FieldCount - startBound;
			}
			if (reader.FieldCount <= startBound)
			{
				//todo: fix error message
				throw new ArgumentException("todo: fix me! When using the multi-mapping APIs ensure you set the splitOn param if you have keys other than Id", "splitOn");
			}

			Type iType;
			iType = item.LocalType;


			// get Properties and Fields of T that we should be able to set
			var properties = GetSettableProperties(iType);
			var fields = GetSettableFields(iType);


			//try {
			il.BeginExceptionBlock();

			// int idx;     // so we know the index into reader that caused us grief.
			LocalBuilder idx = il.DeclareLocal(typeof(int));


			// item = new <T>();    // using public or private constructor.
			il.Emit(OpCodes.Newobj, iType.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, Type.EmptyTypes, null));	// <T>
			il.Emit(OpCodes.Stloc, item);										                                                                                //

			// for struct
			//il.Emit(OpCodes.Ldloca, item);
			//il.Emit(OpCodes.Initobj, iType);

			// for structs this block kills it
			for (int i = startBound; i < startBound + length; i++)
			{
				// get type for reader column(i)
				// is there a matching property/field in item
				//  assign with appropriate casting/boxing etc...

				// ***************************************************************************************************************************************

				// select matching property or field, ordering by properties (case sensitive, case insensitive) then fields (case sensitive, case insensitive)
				var ps = new
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

				// are data types compatible? Is there a direct GetXXX(i) method?
				isAssignable = dstType.IsAssignableFrom(srcType);
				fnGetMethod = typeof(IDataRecord).GetMethod("Get" + srcType.Name, new Type[] { typeof(int) });

				// if(!reader.IsDBNull(i))																	// [Stack]
				// {
				il.Emit(OpCodes.Ldarg_0);																	// reader
				EmitInt32(il, i);																			// reader, i

				// idx = i;
				il.Emit(OpCodes.Dup);																		// reader, i, i
				il.Emit(OpCodes.Stloc, idx);																// reader, i

				il.Emit(OpCodes.Callvirt, fnIsDBNull);														// [bool]
				lblNext = il.DefineLabel();
				il.Emit(OpCodes.Brtrue_S, lblNext);															//


				// "<T>.property/field = (type) reader.GetValue(i);"
				if (isAssignable)
				{
					il.Emit(OpCodes.Ldloc, item);															// <T>
					il.Emit(OpCodes.Ldarg_0);																// <T>, reader 
					EmitInt32(il, i);																		// <T>, reader, i

					if (fnGetMethod != null)
					{
						il.Emit(OpCodes.Callvirt, fnGetMethod);												// <T>, [value as type]
					}
					else
					{
						// getValue, unbox
						il.Emit(OpCodes.Callvirt, fnGetValue);												// <T>, [value as object]
						il.Emit(OpCodes.Unbox_Any, dstType);												// <T>, [value as type]
					}

					if (nullUnderlyingType != null)
					{
						il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));
					}

					if (ps.prop != null)
					{
						il.Emit(OpCodes.Callvirt, ps.prop.Setter);											// <T>
					}
					else
					{
						il.Emit(OpCodes.Stfld, ps.field.Setter);											// <T>
					}
				}
				else
				{
					// Not directly assignable, we'll do some common custom mapping before falling back to calling Convert.ChangeType(..)

					// String/Int => Enum
					if (dstType.IsEnum || (nullUnderlyingType != null && nullUnderlyingType.IsEnum))
					{
						if (IsNumericType(srcType))
						{
							il.Emit(OpCodes.Ldloc, item);														// <T>
							il.Emit(OpCodes.Ldarg_0);														// <T>, reader 
							EmitInt32(il, i);																// <T>, reader, i

							il.Emit(OpCodes.Callvirt, fnGetValue);											// <T>, [value as object]
							il.Emit(OpCodes.Unbox_Any, typeof(int));										// <T>, [value as type]

							if (nullUnderlyingType != null)
							{
								il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));
							}

							if (ps.prop != null)
							{
								il.Emit(OpCodes.Callvirt, ps.prop.Setter);									// 
							}
							else
							{
								il.Emit(OpCodes.Stfld, ps.field.Setter);									// 
							}

						}
						else if (srcType == typeof(string))
						{
							il.Emit(OpCodes.Ldloc, item);														// <T>

							if (nullUnderlyingType != null)
							{
								il.Emit(OpCodes.Ldtoken, nullUnderlyingType);								// <T>, token
							}
							else
							{
								il.Emit(OpCodes.Ldtoken, dstType);											// <T>, token
							}
							il.EmitCall(OpCodes.Call, typeof(Type).GetMethod("GetTypeFromHandle"), null);	// <T>, dstType/nullUnderlyingType

							il.Emit(OpCodes.Ldarg_0);														// <T>, dstType/nullUnderlyingType, reader 
							EmitInt32(il, i);																// <T>, dstType/nullUnderlyingType, reader, i
							il.Emit(OpCodes.Callvirt, fnGetString);											// <T>, dstType/nullUnderlyingType, [value as String]
							EmitInt32(il, 1);																// <T>, dstType/nullUnderlyingType, [value as String], true

							il.EmitCall(OpCodes.Call, fnEnumParse, null);									// <T>, enum
							il.Emit(OpCodes.Unbox_Any, dstType);											// <T>, [enum as dstType]

							if (ps.prop != null)
							{
								il.Emit(OpCodes.Callvirt, ps.prop.Setter);									// 
							}
							else
							{
								il.Emit(OpCodes.Stfld, ps.field.Setter);									// 
							}

						}
					}
					else if (dstType == typeof(Uri) && srcType == typeof(string))
					{
						// String => Uri

						il.Emit(OpCodes.Ldloc, item);															// <T>
						il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
						EmitInt32(il, i);																	// <T>, reader, i

						il.Emit(OpCodes.Callvirt, fnGetString);												// <T>, [string]
						il.Emit(OpCodes.Newobj, typeof(Uri).GetConstructor(new[] { typeof(string) }));		// <T>, [Uri]

						if (ps.prop != null)
						{
							il.Emit(OpCodes.Callvirt, ps.prop.Setter);										// 
						}
						else
						{
							il.Emit(OpCodes.Stfld, ps.field.Setter);										// 
						}

					}
					else if ((dstType == typeof(Guid) || nullUnderlyingType == typeof(Guid)) && srcType == typeof(string))
					{
						// String => Guid

						il.Emit(OpCodes.Ldloc, item);															// <T>
						il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
						EmitInt32(il, i);																	// <T>, reader, i

						il.Emit(OpCodes.Callvirt, fnGetString);												// <T>, [value as string]
						il.EmitCall(OpCodes.Call, fnGuidParse, null);										// <T>, guid

						if (nullUnderlyingType != null)
						{
							il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));
						}

						if (ps.prop != null)
						{
							il.Emit(OpCodes.Callvirt, ps.prop.Setter);										// 
						}
						else
						{
							il.Emit(OpCodes.Stfld, ps.field.Setter);										// 
						}
					}
					else
					{

						// o = reader.GetValue(i);
						il.Emit(OpCodes.Ldloc, item);															// <T>
						il.Emit(OpCodes.Ldarg_0);															// <T>, reader 
						EmitInt32(il, i);																	// <T>, reader, i
						il.Emit(OpCodes.Callvirt, fnGetValue);												// <T>, [value as object]

						//  = (dstType) Convert.ChangeType(o, typeof(dstType/nullUnderlyingType);
						if (nullUnderlyingType != null)
						{
							il.Emit(OpCodes.Ldtoken, nullUnderlyingType);
							il.EmitCall(OpCodes.Call, typeof(Type).GetMethod("GetTypeFromHandle"), null);	// <T>, value, type(nullUnderlyingType)

							il.Emit(OpCodes.Call, fnConvertChangeType);										// <T>, [value as object of nullUnderlyingType]	
							il.Emit(OpCodes.Unbox_Any, nullUnderlyingType);									// <T>, [value as nullUnderlyingType)

							il.Emit(OpCodes.Newobj, dstType.GetConstructor(new[] { nullUnderlyingType }));	// <T>, [value as dstType]

						}
						else
						{
							il.Emit(OpCodes.Ldtoken, dstType);
							il.EmitCall(OpCodes.Call, typeof(Type).GetMethod("GetTypeFromHandle"), null);	// <T>, value, type(dstType)

							il.Emit(OpCodes.Call, fnConvertChangeType);										// <T>, [value as object of dstType]	
							il.Emit(OpCodes.Unbox_Any, dstType);											// <T>, [value as dstType)

						}

						if (ps.prop != null)
						{
							il.Emit(OpCodes.Callvirt, ps.prop.Setter);										// 
						}
						else
						{
							il.Emit(OpCodes.Stfld, ps.field.Setter);										// 
						}

					}
				}
				//}
				il.MarkLabel(lblNext);	// end of if(!reader.IsDBNull(i))

				// ***************************************************************************************************************************************
			}

			//} catch (Exception ex) {
			il.BeginCatchBlock(typeof(Exception));														    // ex

			// db.ThrowDataException(Exception ex, int idx, IDataReader reader);
			il.Emit(OpCodes.Ldloc, idx);																	// ex, idx
			il.Emit(OpCodes.Ldarg_0);                       												// ex, idx, reader

			il.EmitCall(OpCodes.Call, MethodInfo.GetCurrentMethod().DeclaringType.GetMethod("ThrowDataException", BindingFlags.Static | BindingFlags.NonPublic), null);

			// item = null;
			il.Emit(OpCodes.Ldnull);																	    // ex, null
			il.Emit(OpCodes.Stloc, item);																	// ex

			//}
			il.EndExceptionBlock();

		}












		#region 'Old' code, still to refactor



		#region "Custom Object Deserliser(IL) Generation"
		private static MethodInfo fnIsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");
		private static MethodInfo fnGetValue = typeof(IDataRecord).GetMethod("GetValue", new Type[] { typeof(int) });
		private static MethodInfo fnGetString = typeof(IDataRecord).GetMethod("GetString", new Type[] { typeof(int) });
		private static MethodInfo fnEnumParse = typeof(Enum).GetMethod("Parse", new Type[] { typeof(Type), typeof(string), typeof(bool) });
		private static MethodInfo fnGuidParse = typeof(Guid).GetMethod("Parse", new Type[] { typeof(string) });
		private static MethodInfo fnConvertChangeType = typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(Object), typeof(Type) });





		private static void ThrowDataException(Exception ex, int index, IDataReader reader)
		{
			// an exception was thrown/caught in our custome IL deseralise method. re throw with some nice detail.

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
				case TypeCode.Byte:
				case TypeCode.Decimal:
				case TypeCode.Double:
				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.Int64:
				case TypeCode.SByte:
				case TypeCode.Single:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
				case TypeCode.UInt64:
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

		private static bool IsStructure(Type t)
		{
			//return t.IsValueType && !t.IsPrimitive && !t.IsEnum;
			//return t.IsValueType && !t.IsPrimitive && !t.Namespace.StartsWith("System") && !t.IsEnum;

			if (t.IsValueType == true && t.IsEnum == false && t.IsPrimitive == false)
				return true;
			else
				return false;

		}

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
				default:
					if (value >= -128 && value <= 127)
					{
						il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
					}
					else
					{
						il.Emit(OpCodes.Ldc_I4, value);
					}
					break;
			}
		}

		#endregion
		#endregion
	}
}
