using System.Data;
using dksData;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestProject1
{
    [TestClass()]
    public class ExecuteScalar
    {


        [TestMethod()]
        public void Integer()
        {
            using (IDbConnection db = Database.GetOpenConnection("test"))
            {
                var actual = db.ExecuteScalar<int>("select 1");
                Assert.AreEqual(1, actual);
            }
        }

        [TestMethod()]
        public void NullableInteger_null()
        {
            using (IDbConnection db = Database.GetOpenConnection("test"))
            {
                var actual = db.ExecuteScalar<int?>("select null");
                Assert.AreEqual(false, actual.HasValue);

            }
        }

        [TestMethod()]
        public void NullableInteger()
        {
            using (IDbConnection db = Database.GetOpenConnection("test"))
            {
                var actual = db.ExecuteScalar<int?>("select 1");
				Assert.IsTrue(actual.HasValue);
                Assert.AreEqual(1, actual.Value);

            }
        }
    }
}
