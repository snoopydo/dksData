using dksData;
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
    public class ValueTypes
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

        IDbConnection db;
        //Use TestInitialize to run code before running each test
        [TestInitialize()]
        public void MyTestInitialize()
        {
            db = dksData.Database.GetConnection("test");
            db.Open();
        }

        //Use TestCleanup to run code after each test has run
        [TestCleanup()]
        public void MyTestCleanup()
        {
            db.Close();
        }

        #endregion


        #region "Query<ValueTypes>"
        [TestMethod()]
        public void Query_Ints()
        {
            db.Query<int>("select 1 union all select 2 union all select 3").IsSequenceEqualTo(new[] { 1, 2, 3 });
        }

        [TestMethod()]
        public void Query_Strings()
        {
            db.Query<string>("select 'a' union all select 'b' union all select 'c'").IsSequenceEqualTo(new[] { "a", "b", "c" });
        }

        [TestMethod()]
        public void Query_Bools()
        {
            db.Query<bool>("select cast(1 as bit) union all select cast(0 as bit) union all select cast(1 as bit) union all select cast(0 as bit)").IsSequenceEqualTo(new[] { true, false, true, false });
        }

        [TestMethod()]
        public void Query_DateTime()
        {
            db.Query<DateTime>("select cast('1 Jan 2011' as datetime) union all select '2 Feb 2012 13:45' union all select '3 Mar 2013' union all select '4 Apr 2014'").IsSequenceEqualTo(new[] { new DateTime(2011, 1, 1), new DateTime(2012, 2, 2, 13, 45, 0), new DateTime(2013, 3, 3), new DateTime(2014, 4, 4) });
        }


        [TestMethod]
        public void Query_Ints_WithIDbDataParameter()
        {
            db.Query<int>(@"select *
                            from (
	                            select 1 as c union all select 2 union all select 3 union all select 4 as c union all select 5 union all select 6 union all select 7 as c union all select 8 union all select 9
                            ) data
                            where c > @i", new System.Data.SqlClient.SqlParameter("i", 5)).IsSequenceEqualTo(new[] { 6, 7, 8, 9 });
        }

        [TestMethod]
        public void Query_Ints_WithObjectParameter()
        {
            var results = db.Query<int>(@"select *
                            from (
	                            select 15 as c union all select 2 union all select 3 union all select 4 as c union all select 5 union all select 6 union all select 7 as c union all select 8 union all select 9 union all select 99
                            ) data
                            where c > @0 and c < @min and 'aa'=@other", 5, new { min = 20, other = "aa" });

            results.IsSequenceEqualTo(new[] { 15, 6, 7, 8, 9 });
        }


        [TestMethod]
        public void Query_Ints_WithObjectParameterAndSQLParameter()
        {
            var results = db.Query<int>(@"select *
                            from (
	                            select 15 as c union all select 2 union all select 3 union all select 4 as c union all select 5 union all select 6 union all select 7 as c union all select 8 union all select 9 union all select 99
                            ) data
                            where c > @0 and c < @min and 'aa'=@other and @i=5", 5, new { other = "aa", min = 20 }, new System.Data.SqlClient.SqlParameter("i", 5));

            results.IsSequenceEqualTo(new[] { 15, 6, 7, 8, 9 });
        }
        #endregion


        #region "ExecuteScalar<ValueTypes>"

        [TestMethod()]
        public void ExecuteScalar_Int()
        {
            Assert.AreEqual<int>(1, db.ExecuteScalar<int>("select 1, 2 union all select 3,4"));
        }
        [TestMethod()]
        public void ExecuteScalar_String()
        {
            Assert.AreEqual<string>("Apple", db.ExecuteScalar<string>("select 'Apple', 'Bannana' union all select 'Flower','Friday'"));
        }
        #endregion


        #region "ExecuteNonQuery"
        [TestMethod()]
        public void ExecuteNonQuery()
        {
            // db.CreateCommand unescapes @@variable
            Assert.AreEqual<int>(2, db.ExecuteNonQuery("declare @@t table(i1 int, i2 int); insert into @@t(i1,i2) select 1, 2 union all select 3,4"));


            var results = db.Query<int>("declare @@t table(i1 int, i2 int); insert into @@t(i1,i2) select 1, 2 union all select 3,4; select i1 from @@t");
            results.IsSequenceEqualTo(new[] { 1, 3 });
        }
        #endregion
    }
}
