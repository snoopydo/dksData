using System;
using System.Collections.Concurrent;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

namespace dksData
{
    public static class Database
    {

        // todo: Implement cache expiry, clean up etc...
        // todo: implement parameters for commandTimeout, commandType, openTransaction...?

        #region GetConnection(...)
        // cache functions to create appropriate IDbConnection.
        private static ConcurrentDictionary<string, Func<IDbConnection>> dbFactoryCache = new ConcurrentDictionary<string, Func<IDbConnection>>();

        public static IDbConnection GetConnection()
        {
            string connectionStringName;

            connectionStringName = ConfigurationManager.ConnectionStrings[0].Name;

            return GetConnection(connectionStringName);
        }

        public static IDbConnection GetConnection(string connectionStringName)
        {
            Func<IDbConnection> createConnection;

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
                        IDbConnection db;
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
        #endregion

        #region ExecuteScalar(...), ExecuteNonQuery(...)

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

        #endregion

        #region Query(...)

        // Query<T>(this IDbConnection db, string sql, params object[] parameters)
        // Query<TRet>(this IDbConnection db, Type[] types, object callback, string sql, params object[] parameters)


        public static IEnumerable<T> Query<T>(this IDbConnection db, string sql, params object[] parameters)
        {
            
            using (var cmd = CreateCommand(db, sql, parameters))
            {
            
                using (var reader = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                {
                
                    var deserialiser = GetDeserliser<T>(reader);

                    while (reader.Read())
                    {
                        yield return deserialiser(reader);
                    }
                    
                    reader.Close();

                }

            }

        }


        #endregion


        private static Func<IDataReader, T> GetDeserliser<T>(IDataReader reader)
        {
            
            // using IL generator, create a dynamic method that maps columns in reader to properties/fields in T


            return null;
        }



        #region Internal Stuff
        

        public static IDbCommand CreateCommand(IDbConnection db, string sql, params object[] parameters)
        {
            IDbCommand cmd;

            // handle named/numbered etc parameters, fixing sql if required.
            var new_parameters = new List<object>();
            sql = ParseParameters(sql, parameters, new_parameters);
            parameters = new_parameters.ToArray();


            cmd = db.CreateCommand();

            cmd.CommandText = sql;
            //cmd.CommandTimeout = commandTimeout;
            //cmd.CommandType = commandType;
            //cmd.Transaction = transaction;

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



        // flogged from TopTenSoftwares' PetaPoco
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

        #endregion
    }
}
