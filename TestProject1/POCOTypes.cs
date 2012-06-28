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
    public class POCOTypes
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
            db = dksData.Database.GetOpenConnection("test");
        }

        //Use TestCleanup to run code after each test has run
        [TestCleanup()]
        public void MyTestCleanup()
        {
            db.Close();
        }

        #endregion



        // test hydrating POCOs
        public class Dog
        {
            public int? Age { get; set; }
            public Guid Id { get; set; }
            public string Name { get; set; }
            public float? Weight { get; set; }

            public int IgnoredProperty { get { return 1; } }
        }

        [TestMethod()]
        public void TestDog()
        {
            var guid = Guid.NewGuid();
            var dog = db.Query<Dog>("select '' as Extra, 1 as Age, 0.1 as Name1 , Id = @id", new { Id = guid }).ToList();

            dog.Count()
               .IsEqualTo(1);

            dog.First().Age
                .IsEqualTo(1);

            dog.First().Id
                .IsEqualTo(guid);
        }

        class Cat
        {
            public int? Age { get; set; }
            public Guid Id { get; set; }
            public string Name { get; set; }
            public float? Weight { get; set; }

            public int IgnoredProperty { get { return 1; } }
        }

        private Cat d()
        {
            return new Cat();
        }

        [TestMethod()]
        public void TestCat()
        {
            var guid = Guid.NewGuid();
            var cat = db.Query<Cat>("select '' as Extra, 1 as Age, 0.1 as Name1 , Id = @id", new { Id = guid }).ToList();

            cat.Count()
               .IsEqualTo(1);

            cat.First().Age
                .IsEqualTo(1);

            cat.First().Id
                .IsEqualTo(guid);
        }

    }
}
