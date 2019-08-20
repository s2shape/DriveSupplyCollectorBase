using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DriveSupplyCollectorBase.FileProcessors;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;
using Xunit.Abstractions;

namespace DriveSupplyCollectorBaseTests
{
    public class CsvTests
    {
        private readonly ITestOutputHelper output;

        public CsvTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void TestCsvDetect() {
            var processor = new CsvFileProcessor();
            
            Assert.True(processor.CanProcess("emails-utf8.csv"));
        }

        [Fact]
        public void TestCsvSchema() {
            var fields = new Dictionary<string, DataType>() {
                {"ID", DataType.Guid},
                {"DELETED", DataType.Boolean},
                {"CREATED_BY", DataType.Guid},
                {"DATE_ENTERED", DataType.DateTime},
                {"MODIFIED_USER_ID", DataType.Guid},
                {"DATE_MODIFIED", DataType.DateTime},
                {"DATE_MODIFIED_UTC", DataType.DateTime},
                {"ASSIGNED_USER_ID", DataType.Guid},
                {"TEAM_ID", DataType.String},
                {"NAME", DataType.String},
                {"DATE_START", DataType.DateTime},
                {"TIME_START", DataType.DateTime},
                {"PARENT_TYPE", DataType.String},
                {"PARENT_ID", DataType.Guid},
                {"DESCRIPTION", DataType.String},
                {"DESCRIPTION_HTML", DataType.String},
                {"FROM_ADDR", DataType.String},
                {"FROM_NAME", DataType.String},
            };

            var container = new DataContainer();
            var collection = new DataCollection(container, "emails-utf8.csv");

            long rowCount;
            var processor = new CsvFileProcessor();
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/emails-utf8.csv", FileMode.Open)) {
                entities = processor.ParseFileSchema(container, collection, stream, out rowCount);
            }

            Assert.Equal(200, rowCount);

            foreach (var field in fields) {
                output.WriteLine($" check field {field.Key}");
                var entity = entities.Find(x => x.Name.Equals(field.Key));
                Assert.NotNull(entity);
                Assert.Equal(field.Value, entity.DataType);
            }
        }

        [Fact]
        public void TestCsvCollect()
        {
            var container = new DataContainer();
            var collection = new DataCollection(container, "emails-utf8.csv");

            var processor = new CsvFileProcessor();

            long rowCount;
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/emails-utf8.csv", FileMode.Open))
            {
                entities = processor.ParseFileSchema(container, collection, stream, out rowCount);
            }

            var entity = entities.FirstOrDefault(x => x.Name.Equals("FROM_ADDR"));

            int index = entities.IndexOf(entity);
            
            using (var stream = File.Open("../../../tests/emails-utf8.csv", FileMode.Open)) {
                var samples =
                    processor.CollectSamples(container, collection, entity, index, stream, 5);
                Assert.Contains("sally@example.com", samples);
            }
        }

        [Fact]
        public void TestCsvEncodings() {
            var container = new DataContainer();
            var collection1 = new DataCollection(container, "emails-utf8.csv");
            var collection2 = new DataCollection(container, "emails-utf16.csv");

            var emails = new string[] { "will@example.com", "sally@example.com", "chris@example.com" };

            var processor = new CsvFileProcessor();

            long rowCount;
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/emails-utf8.csv", FileMode.Open))
            {
                entities = processor.ParseFileSchema(container, collection1, stream, out rowCount);
            }
            var entity = entities.FirstOrDefault(x => x.Name.Equals("FROM_ADDR"));
            int index = entities.IndexOf(entity);

            List<string> samples1;
            List<string> samples2;

            using (var stream = File.Open("../../../tests/emails-utf8.csv", FileMode.Open)) {
                samples1 = processor.CollectSamples(container, collection1, entity, index, stream,
                    emails.Length);
            }

            using (var stream = File.Open("../../../tests/emails-utf16.csv", FileMode.Open)) {
                samples2 = processor.CollectSamples(container, collection2, entity, index, stream,
                    emails.Length);
            }

            Assert.Equal(emails.Length, samples1.Count);
            Assert.Equal(emails.Length, samples2.Count);

            for (int i = 0; i < emails.Length; i++) {
                Assert.Equal(emails[i], samples1[i]);
                Assert.Equal(emails[i], samples2[i]);
            }
        }
    }
}
