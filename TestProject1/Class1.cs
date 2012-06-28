using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using dksData;

namespace TestProject1
{
    public class Class1
    {

        class testC
        {
            public string Extra;
            public Guid id;
        }

        struct testS
        {
            public string Extra;
            public Guid id;
        }

        public void ClassTest()
        {
            var db = dksData.Database.GetConnection("test");
            db.Open();

            var ds = db.ExecuteReader("select '' as Extra, 1 as Age, 0.1 as Name1 , Id = @id", new { Id = Guid.NewGuid() });
            testC item;

            while (ds.Read())
            {
                item = new testC();
                item.Extra = ds.GetString(0);
                item.id = ds.GetGuid(3);
            }

            ds.Close();
            db.Close();
        }

        public void ClassStruct()
        {
            var db = dksData.Database.GetConnection("test");
            db.Open();

            var ds = db.ExecuteReader("select '' as Extra, 1 as Age, 0.1 as Name1 , Id = @id", new { Id = Guid.NewGuid() });
            testS item;
            while (ds.Read())
            {
                item.Extra = ds.GetString(0);
                item.id = ds.GetGuid(3);
            }

            ds.Close();
            db.Close();
        }

    }
}
