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

        [Fact]
        public void TestNestedJson() {
            var container = new DataContainer();
            var collection = new DataCollection(container, "nested.json");

            var processor = new JsonFileProcessor();
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/nested.json", FileMode.Open))
            {
                entities = processor.ParseFileSchema(container, collection, stream);
            }

            var entity1 = entities.Find(x => x.Name.Equals("Addresses.Home.City"));
            Assert.NotNull(entity1);

            var entity2 = entities.Find(x => x.Name.Equals("Addresses.Work.Street1"));
            Assert.NotNull(entity2);

            var entity3 = entities.Find(x => x.Name.Equals("Languages.Name"));
            Assert.NotNull(entity3);

            var entity4 = entities.Find(x => x.Name.Equals("WorkHistory.NewSkills"));
            Assert.NotNull(entity4);

            using (var stream = File.Open("../../../tests/nested.json", FileMode.Open)) {
                var samples = processor.CollectSamples(container, collection, entity1, 0, stream, 10);
                Assert.Equal(1, samples.Count);
                Assert.Equal("Sochi", samples[0]);
            }

            using (var stream = File.Open("../../../tests/nested.json", FileMode.Open)) {
                var samples = processor.CollectSamples(container, collection, entity2, 0, stream, 10);
                Assert.Equal(1, samples.Count);
                Assert.Equal("Seversk, Kalinina st", samples[0]);
            }

            using (var stream = File.Open("../../../tests/nested.json", FileMode.Open)) {
                var samples = processor.CollectSamples(container, collection, entity3, 0, stream, 10);
                Assert.Equal(2, samples.Count);
                Assert.Contains("English", samples);
                Assert.Contains("Russian", samples);
            }

            using (var stream = File.Open("../../../tests/nested.json", FileMode.Open)) {
                var samples = processor.CollectSamples(container, collection, entity4, 0, stream, 10);
                Assert.Equal(5, samples.Count);
                Assert.Contains("Agile", samples);
                Assert.Contains("Scrum", samples);
                Assert.Contains("TeamCity", samples);
                Assert.Contains("HL7", samples);
                Assert.Contains("EMR", samples);
            }
        }
    }
}
