using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using DriveSupplyCollectorBase.FileProcessors;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;
using Xunit.Abstractions;

namespace DriveSupplyCollectorBaseTests
{
    public class JsonTests
    {
        private readonly ITestOutputHelper output;

        public JsonTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void TestJsonDetect()
        {
            var processor = new JsonFileProcessor();

            Assert.True(processor.CanProcess("emails-utf8.json"));
        }

        [Fact]
        public void TestJsonSchema()
        {
            var fields = new Dictionary<string, DataType>() {
                {"id", DataType.String},
                {"deleted", DataType.Boolean},
                {"created.user", DataType.String},
                {"created.date", DataType.DateTime},
                {"modified.user", DataType.String},
                {"modified.date", DataType.DateTime},
                {"modified.date_utc", DataType.DateTime},
                {"assigned_user_id", DataType.String},
                {"team_id", DataType.Unknown},
                {"name", DataType.String},
                {"start.date", DataType.DateTime},
                {"start.time", DataType.DateTime},
                {"parent.type", DataType.String},
                {"parent.id", DataType.String},
                {"description.text", DataType.String},
                {"description.html", DataType.Unknown},
                {"from.addr", DataType.String},
                {"from.name", DataType.String},
            };

            var container = new DataContainer();
            var collection = new DataCollection(container, "emails-utf8.json");

            var processor = new JsonFileProcessor();
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/emails-utf8.json", FileMode.Open)) {
                entities = processor.ParseFileSchema(container, collection, stream);
            }

            foreach (var field in fields)
            {
                output.WriteLine($" check field {field.Key}");
                var entity = entities.Find(x => x.Name.Equals(field.Key));
                Assert.NotNull(entity);
                Assert.Equal(field.Value, entity.DataType);
            }
        }

        [Fact]
        public void TestJsonCollect()
        {
            var container = new DataContainer();
            var collection = new DataCollection(container, "emails-utf8.json");

            var processor = new JsonFileProcessor();
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/emails-utf8.json", FileMode.Open))
            {
                entities = processor.ParseFileSchema(container, collection, stream);
            }

            var entity = entities.FirstOrDefault(x => x.Name.Equals("from.addr"));
            int index = entities.IndexOf(entity);
            
            using (var stream = File.Open("../../../tests/emails-utf8.json", FileMode.Open)) {
                var samples = processor.CollectSamples(container, collection, entity, index, stream, 5);
                Assert.Contains("sally@example.com", samples);
            }
        }
    }
}
