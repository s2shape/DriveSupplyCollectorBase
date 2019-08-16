using System;
using System.Collections.Generic;
using DriveSupplyCollectorBase.FileTypeResolvers;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;
using Xunit.Abstractions;

namespace DriveSupplyCollectorBaseTests
{
    public class DriveSupplyCollectorBaseTests
    {
        private readonly ITestOutputHelper output;

        public DriveSupplyCollectorBaseTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void TestCsvDetect() {
            var resolver = new CsvFileTypeResolver();
            
            Assert.True(resolver.CanProcess("emails-utf8.csv"));
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

            var resolver = new CsvFileTypeResolver();
            var entities = resolver.ParseFileSchema(container, collection, "../../../tests/emails-utf8.csv");

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
            var entity = new DataEntity("FROM_ADDR", DataType.String, "String", container, collection);

            var resolver = new CsvFileTypeResolver();
            var samples = resolver.CollectSamples(container, collection, entity, "../../../tests/emails-utf8.csv", 5);
            Assert.Contains("sally@example.com", samples);
        }

        [Fact]
        public void TestCsvEncodings() {
            var container = new DataContainer();
            var collection1 = new DataCollection(container, "emails-utf8.csv");
            var collection2 = new DataCollection(container, "emails-utf16.csv");

            var entity = new DataEntity("FROM_ADDR", DataType.String, "String", container, collection1);

            var emails = new string[] { "will@example.com", "sally@example.com", "chris@example.com" };

            var resolver = new CsvFileTypeResolver();
            var samples1 = resolver.CollectSamples(container, collection1, entity, "../../../tests/emails-utf8.csv", emails.Length);
            var samples2 = resolver.CollectSamples(container, collection2, entity, "../../../tests/emails-utf16.csv", emails.Length);

            Assert.Equal(emails.Length, samples1.Count);
            Assert.Equal(emails.Length, samples2.Count);

            for (int i = 0; i < emails.Length; i++) {
                Assert.Equal(emails[i], samples1[i]);
                Assert.Equal(emails[i], samples2[i]);
            }
        }

    }
}
