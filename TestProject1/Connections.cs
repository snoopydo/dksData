using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Configuration;
using System.Linq;

namespace TestProject1
{
    [TestClass()]
    public class Connections
    {
        [TestMethod()]
        public void GetConnection()
        {
            string connectionStringName;
            string expectedConnectionString;
            IDbConnection actual;

            connectionStringName = "test";
			expectedConnectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;

            actual = dksData.Database.GetConnection(connectionStringName);

            // we got a IDbConnection
            Assert.IsNotNull(actual);

            // its using the correct connection string
            Assert.AreEqual(actual.ConnectionString, expectedConnectionString);

            // it should be closed still
            Assert.AreEqual(ConnectionState.Closed, actual.State);

            // we should be able to open it
            actual.Open();
            Assert.AreEqual(ConnectionState.Open, actual.State);

            // and close it again.
            actual.Close();
            Assert.AreEqual(ConnectionState.Closed, actual.State);
            
        }

        [TestMethod()]
        public void GetOpenConnection()
        {
            string connectionStringName;
            string expectedConnectionString;
            IDbConnection actual;

            connectionStringName = "test";
            expectedConnectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;

            actual = dksData.Database.GetOpenConnection(connectionStringName);

            // we got a IDbConnection
            Assert.IsNotNull(actual);

            // it should already be open
            Assert.AreEqual(ConnectionState.Open, actual.State);

            // and close it.
            actual.Close();
            Assert.AreEqual(ConnectionState.Closed, actual.State);

        }
      
    }
}
