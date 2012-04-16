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
        public void GetConnection_DefaultConnectionString()
        {
            System.Diagnostics.Debug.WriteLine("GetConnection_DefaultConnectionString");

            string expectedConnectionString;
            IDbConnection actual;

            expectedConnectionString = ConfigurationManager.ConnectionStrings[0].ConnectionString;

            actual = dksData.Database.GetConnection();

            Assert.IsNotNull(actual);
            Assert.AreEqual(actual.ConnectionString, expectedConnectionString);

            // the default connection is normally a local sqlexpress connection string refering to aspnetdb.mdf in data directory which we probably dont have
            //actual.Open();
            //actual.Close();
        }

        [TestMethod()]
        public void GetConnection_NamedConnectionString()
        {
            System.Diagnostics.Debug.WriteLine("GetConnection_NamedConnectionString");

            string connectionStringName;
            string expectedConnectionString;
            IDbConnection actual;

            connectionStringName = "test";
			expectedConnectionString = ConfigurationManager.ConnectionStrings["test"].ConnectionString;

            actual = dksData.Database.GetConnection(connectionStringName);

            Assert.IsNotNull(actual);
            Assert.AreEqual(actual.ConnectionString, expectedConnectionString);

            // being a named connection, we should be able to open and close it.
            actual.Open();
            actual.Close();

        }

      
    }
}
