using System;
using System.Collections.Generic;
using System.Text;
using DriveSupplyCollectorBase.FileTypeResolvers;
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
            var resolver = new JsonFileTypeResolver();

            Assert.True(resolver.CanProcess("emails-utf8.json"));
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

            var resolver = new JsonFileTypeResolver();
            var entities = resolver.ParseFileSchema(container, collection, "../../../tests/emails-utf8.json");

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
            var entity = new DataEntity("from.addr", DataType.String, "String", container, collection);

            var resolver = new JsonFileTypeResolver();
            var samples = resolver.CollectSamples(container, collection, entity, "../../../tests/emails-utf8.json", 5);
            Assert.Contains("sally@example.com", samples);
        }
    }
}
