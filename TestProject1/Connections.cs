using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Data;
using System.Configuration;
using System.Linq;

namespace TestProject1
{


    /// <summary>
    ///This is a test class for DatabaseTest and is intended
    ///to contain all DatabaseTest Unit Tests
    ///</summary>
    [TestClass()]
    public class Connections
    {


        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //

        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //

        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}

        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}

        #endregion

        [TestMethod()]
        public void Connection_GetConnection()
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
        public void Connection_GetOpenConnection()
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
