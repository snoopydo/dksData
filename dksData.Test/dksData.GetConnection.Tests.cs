using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Configuration;
using System.Linq;
using System.Data.SqlClient;

namespace TestProject1
{
    [TestClass()]
    public class Connections
    {

		[TestCategory("GetConnection")]
        [TestMethod()]
        public void GetConnection_1st_Run()
        {
			GetConnection();      
        }
		
		[TestCategory("GetConnection")]
		[TestMethod()]
		public void GetConnection_2nd_Run()
		{
			GetConnection();      
		}


		[TestCategory("GetConnection")]
		[TestMethod()]
        public void GetOpenConnection_1st_Run()
        {
			GetOpenConnection();
        }

		[TestCategory("GetConnection")]
		[TestMethod()]
		public void GetOpenConnection_2nd_Run()
		{
			GetOpenConnection();
		}
		
		
		[TestCategory("GetConnection")]
		[TestMethod()]
		public void GetConnection_Old_1st_Run()
		{
			GetConnection_Old();
		}

		[TestCategory("GetConnection")]
		[TestMethod()]
		public void GetConnection_Old_2nd_Run()
		{
			GetConnection_Old();

		}

		private void GetConnection()
		{
			string connectionStringName;
			string expectedConnectionString;

			connectionStringName = "test";
			expectedConnectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;

			using (var actual = dksData.Database.GetConnection(connectionStringName))
			{

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
		}

		private void GetOpenConnection()
		{
			string connectionStringName;
			string expectedConnectionString;

			connectionStringName = "test";
			expectedConnectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;

			using (var actual = dksData.Database.GetOpenConnection(connectionStringName))
			{

				// we got a IDbConnection
				Assert.IsNotNull(actual);

				// it should already be open
				Assert.AreEqual(ConnectionState.Open, actual.State);

				// and close it.
				actual.Close();
				Assert.AreEqual(ConnectionState.Closed, actual.State);
			}
		}

		private void GetConnection_Old()
		{

			string connectionStringName;
			string expectedConnectionString;

			connectionStringName = "test";
			expectedConnectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;

			using (var actual = new SqlConnection(expectedConnectionString))
			{
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


		}
    }
}
