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







        private static Func<IDataReader, T> GetDeserliser<T>(string key, IDataReader reader, int startBound, int length)
        {
            return null;
        }

        private static void GenerateMethodBody<T>(ILGenerator il, IDataReader reader, int startBound, int length)
        {
        }
      

    }
}