/*

 dksData	- A very simple, sql data access object mapper for .Net
 
 Version	- 18 January, 2012
 
 Links
 * SubSonic	- http://www.subsonicproject.com/
 * Massive	- https://github.com/robconery/massive
 * PetaPoco	- http://www.toptensoftware.com/petapoco/
 * Dapper	- http://code.google.com/p/dapper-dot-net/
  
 
 */

#region "VB Test Code for Parent/Child mapping"
#if false
	Class HostUrl
		Public Id As Integer
		Public HostId As Integer
		Public Url As Uri
	End Class
	Class Host
		Public Id As Integer
		Public Enabled As Byte
		Public Host As Uri
		Public Urls As List(Of HostUrl)
	End Class


	Class HostUrlMapper
		Dim current As Host
		Public Function MapIt(h As Host, u As HostUrl) As Host
			If h Is Nothing Then Return current

			If current IsNot Nothing AndAlso current.Id = h.Id Then
				current.Urls.Add(u)
				Return Nothing
			End If

			Dim prev = current

			current = h
			current.Urls = New List(Of HostUrl)
			current.Urls.Add(u)

			Return prev

		End Function
	End Class

	Sub Main()
		Dim sql As String

		sql = "select h.id, h.enabled, h.host, u.id, u.hostid, u.url"
		sql += " from hosts h"
		sql += " left join ("
		sql += " select u.*, row=row_number() over(partition by u.hostid order by u.id)"
		sql += " from urls u"
		sql += " ) u on h.id=u.hostid"
		sql += " where h.id in (1,8,20)"
		sql += "  and u.row <= 5"

		Dim hosts = Data.DB.QueryMapped(Of Host, HostUrl, Host)(AddressOf (New HostUrlMapper()).MapIt, sql)

		For Each Host In hosts
			Console.WriteLine("Host: {0}: {1}", Host.Id, Host.Host)
			For Each HostUrl In Host.Urls
				Console.WriteLine("	Url: {0} {2}: {1}", HostUrl.Id, HostUrl.Url, HostUrl.HostId)
			Next
			Console.WriteLine("")
		Next

		Console.ReadLine()
	End Sub
#endif
#endregion

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
using System.Text.RegularExpressions;
using System.Text;
using dksData;

namespace dksData2
{
    public static class Database
    {

        

        #region "ExecuteNonQuery(...)"
        public static int ExecuteNonQuery(this IDbConnection db, string sql)
        {
            int result;

            using (var cmd = db.CreateCommand())
            {
                cmd.CommandText = sql;

                result = cmd.ExecuteNonQuery();

                cmd.Dispose();
            }
            return result;
        }
        #endregion

        #region "Internal Reader Deserliser"
        private static Func<IDataReader, T> GetDeserliser<T>(this IDataReader reader)
        {
            Type type = typeof(T);
            
            if (type.IsValueType || type == typeof(string))
            {
                return GetValueTypeDeserliser<T>(reader);
             }
            else 
            {
                return GetClassDeserliser<T>(reader);
            }

        }

        private static Func<IDataReader, T> GetClassDeserliser<T>(IDataReader reader)
        {
            var dm = new DynamicMethod(string.Format("Deserialize{0}", Guid.NewGuid()), typeof(T), new[] { typeof(IDataReader) }, true);
            var il = dm.GetILGenerator();
            ParameterBuilder rdr = dm.DefineParameter(1, ParameterAttributes.None, "rdr");

            GenerateMethodBody<T>(il, reader, 0, -1);

            var factory = (Func<IDataReader, T>)dm.CreateDelegate(typeof(Func<IDataReader, T>));

            return factory as Func<IDataReader, T>;
        }

        private static Func<IDataReader, T> GetValueTypeDeserliser<T>(IDataReader reader)
        {
            return rdr =>
            {
                return (T)rdr.GetValue(0);
            };
        }
        #endregion




        public static IEnumerable<T> PerfTest<T>(IDbConnection db, string sql, params System.Data.IDbDataParameter[] parameters)
        {
            using (var dc = db.CreateCommand())
            {
                dc.Connection = db;
                dc.CommandText = sql;
                dc.CommandType = CommandType.Text;

                //if (timeout > 0) { dc.CommandTimeout = timeout; }

                foreach (IDbDataParameter param in parameters) { dc.Parameters.Add(param); }

                using (var reader = dc.ExecuteReader())
                {
                    var deserialiser = GetDeserliser<T>(typeof(T).ToString() + '-' + dc.CommandText + '-' + db.ConnectionString, reader, 0, -1);

                    //SaveAssembly<T>(reader);

                    while (reader.Read())
                    {
                        yield return deserialiser(reader);
                    }
                }
            }
        }

        public static void Save<T>(IDbConnection db, string sql, params System.Data.IDbDataParameter[] parameters)
        {
            using (var dc = db.CreateCommand())
            {
                dc.Connection = db;
                dc.CommandText = sql;
                dc.CommandType = CommandType.Text;

                foreach (IDbDataParameter param in parameters) { dc.Parameters.Add(param); }

                using (var reader = dc.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    SaveAssembly<T>(reader);
                }
            }
        }

        private static void SaveAssembly<T>(IDataReader reader)
        {
            // http://msdn.microsoft.com/en-us/library/8zwdfdeh.aspx

            string typeName = typeof(T).ToString().Replace(".", "").Replace("+", "");
            AssemblyName assemblyName = new AssemblyName();
            assemblyName.Name = "dksData-deserialiser-" + typeName;
            assemblyName.Version = new Version(1, 0, 0, 0);

            AssemblyBuilder assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.RunAndSave);

            //Type daType = typeof(DebuggableAttribute);
            //ConstructorInfo daCtor = daType.GetConstructor(new Type[] { typeof(DebuggableAttribute.DebuggingModes) });
            //CustomAttributeBuilder daBuilder = new CustomAttributeBuilder(daCtor, new object[] { 
            //                                            DebuggableAttribute.DebuggingModes.DisableOptimizations | 
            //                                            DebuggableAttribute.DebuggingModes.Default });
            //assemblyBuilder.SetCustomAttribute(daBuilder);


            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule("MyDynamicModule.dll", "MyDynamicModule.dll", true);

            TypeBuilder typeBuilder = moduleBuilder.DefineType(typeName);

            MethodBuilder mb = typeBuilder.DefineMethod("DeseriliseMethodDynamic", MethodAttributes.Public, typeof(T), new[] { typeof(IDataReader) });
            ParameterBuilder rdr = mb.DefineParameter(1, ParameterAttributes.None, "rdr");

            ILGenerator il = mb.GetILGenerator();

            GenerateMethodBody<T>(il, reader, 0, -1);

            var t = typeBuilder.CreateType();

            if (System.IO.File.Exists("MyDynamicModule.dll")) { System.IO.File.Delete("MyDynamicModule.dll"); }
            assemblyBuilder.Save("MyDynamicModule.dll");

        }



        #region "Private Implementations"


        private static System.Data.IDataReader ExecuteReaderInternal(string connection, CommandType type, CommandBehavior commandBehavior, string sql, int timeout, params System.Data.IDbDataParameter[] parameters)
        {
            IDbConnection db = null;
            IDbCommand dc = null;
            IDataReader reader;

            try
            {
                //todo: using connectionStrings provider name, create appropriate connection...
                db = dksData.Database.GetConnection(connection);
                db.Open();

                dc = db.CreateCommand();
                dc.Connection = db;
                dc.CommandText = sql;
                dc.CommandType = type;

                if (timeout > 0) { dc.CommandTimeout = timeout; }

                foreach (IDbDataParameter param in parameters) { dc.Parameters.Add(param); }

                reader = dc.ExecuteReader(commandBehavior);
                return reader;
            }
            catch
            {
                if (dc != null) { dc.Dispose(); }
                if (db != null && db.State != ConnectionState.Closed) { db.Dispose(); }
                throw;
            }

        }

        //private static IEnumerable<T> QueryInternal<T>(string connection, CommandType type, string sql, int timeout, params System.Data.IDbDataParameter[] parameters) where T : new()
        private static IEnumerable<T> QueryInternal<T>(string connection, CommandType type, string sql, int timeout, params System.Data.IDbDataParameter[] parameters)
        {
            using (var db = dksData.Database.GetConnection(connection))
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

                        //SaveAssembly<T>(reader);

                        while (reader.Read())
                        {
                            yield return deserialiser(reader);
                        }
                    }

                }

            }

        }

        #endregion








        #region "Custom Object Deserliser(IL) Generation"
        private static MethodInfo fnIsDBNull = typeof(IDataRecord).GetMethod("IsDBNull");
        private static MethodInfo fnGetValue = typeof(IDataRecord).GetMethod("GetValue", new Type[] { typeof(int) });
        private static MethodInfo fnGetString = typeof(IDataRecord).GetMethod("GetString", new Type[] { typeof(int) });
        private static MethodInfo fnEnumParse = typeof(Enum).GetMethod("Parse", new Type[] { typeof(Type), typeof(string), typeof(bool) });
        private static MethodInfo fnGuidParse = typeof(Guid).GetMethod("Parse", new Type[] { typeof(string) });
        private static MethodInfo fnConvertChangeType = typeof(Convert).GetMethod("ChangeType", new Type[] { typeof(Object), typeof(Type) });

        private static Dictionary<string, object> pocoFactories = new Dictionary<string, object>();

        //private static Func<IDataReader, T> GetDeserliser<T>(string key, IDataReader reader, int startBound, int length) where T : new()
        private static Func<IDataReader, T> GetDeserliser<T>(string key, IDataReader reader, int startBound, int length)
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

            Type iType = typeof(T);

            // simple type?
            if (iType.IsValueType || iType == typeof(string) || iType == typeof(byte[]))
            {
                return (rdr) => (T)rdr.GetValue(0);
            }
            else
            {
                var dm = new DynamicMethod(string.Format("Deserialize{0}", Guid.NewGuid()), iType, new[] { typeof(IDataReader) }, true);
                var il = dm.GetILGenerator();
                ParameterBuilder rdr = dm.DefineParameter(1, ParameterAttributes.None, "rdr");

                GenerateMethodBody<T>(il, reader, startBound, length);

                // cache custom method
                var factory = (Func<IDataReader, T>)dm.CreateDelegate(typeof(Func<IDataReader, T>));
                lock (pocoFactories)
                {
                    pocoFactories[key] = factory;
                }

                return factory as Func<IDataReader, T>;
            }
        }

        private static void GenerateMethodBody<T>(ILGenerator il, IDataReader reader, int startBound, int length)
        {
            Type iType = typeof(T);

            // get Properties and Fields of T that we should be able to set
            var properties = iType
                    .GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                    .Select(p => new
                    {
                        p.Name,
                        Setter = p.DeclaringType == typeof(T) ? p.GetSetMethod(true) : p.DeclaringType.GetProperty(p.Name).GetSetMethod(true),
                        PropertyType = p.PropertyType
                    })
                    .Where(info => info.Setter != null)
                    .ToList();

            var fields = iType.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                    .Select(f => new
                    {
                        f.Name,
                        Setter = f,
                        PropertyType = f.FieldType
                    })
                    .ToList();

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

            // <T> item;
            LocalBuilder item = il.DeclareLocal(iType);
            // int idx;
            LocalBuilder idx = il.DeclareLocal(typeof(int));
            //item.SetLocalSymInfo("item");
            //idx.SetLocalSymInfo("idx");
            //il.MarkSequencePoint(null, 1, 0, 1, 100);

            //try {
            il.BeginExceptionBlock();

            // item = new <T>();    // using public or private constructor.
            il.Emit(OpCodes.Newobj, iType.GetConstructor(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, Type.EmptyTypes, null));	// <T>
            il.Emit(OpCodes.Stloc, item);																		//

            for (int i = startBound; i < startBound + length; i++)
            {
                // select matching property or field, ordering by properties (case sensitive, case insensitive) then fields (case sensitive, case insensitive)
                var ps = new
                {
                    prop = properties.FirstOrDefault(p => string.Equals(p.Name, reader.GetName(i), StringComparison.InvariantCulture)) ?? properties.FirstOrDefault(p => string.Equals(p.Name, reader.GetName(i), StringComparison.InvariantCultureIgnoreCase)),
                    field = fields.FirstOrDefault(f => string.Equals(f.Name, reader.GetName(i), StringComparison.InvariantCulture)) ?? fields.FirstOrDefault(f => string.Equals(f.Name, reader.GetName(i), StringComparison.InvariantCultureIgnoreCase)),
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
            }


            //} catch (Exception ex) {
            il.BeginCatchBlock(typeof(Exception));														// ex

            // call db.ThrowDataException(Exception ex, int idx, IDataReader reader);
            il.Emit(OpCodes.Ldloc, idx);																	// ex, idx
            il.Emit(OpCodes.Ldarg_0);																	// ex, idx, reader

            il.EmitCall(OpCodes.Call, MethodInfo.GetCurrentMethod().DeclaringType.GetMethod("ThrowDataException", BindingFlags.Static | BindingFlags.NonPublic), null);

            // item = null;
            il.Emit(OpCodes.Ldnull);																	// ex, null
            il.Emit(OpCodes.Stloc, item);																	// ex

            //}
            il.EndExceptionBlock();

            // return item;
            il.Emit(OpCodes.Ldloc, item);
            il.Emit(OpCodes.Ret);

        }

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

    }
}