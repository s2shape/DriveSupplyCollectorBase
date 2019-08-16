using System;
using System.Collections.Generic;
using System.Text;
using DriveSupplyCollectorBase.FileTypeResolvers;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;
using Xunit.Abstractions;

namespace DriveSupplyCollectorBaseTests
{
    public class ParquetTests
    {
        private readonly ITestOutputHelper output;

        public ParquetTests(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void TestParquetDetect()
        {
            var resolver = new ParquetFileTypeResolver();

            Assert.True(resolver.CanProcess("emails-utf8.parquet"));
        }

        [Fact]
        public void TestParquetSchema()
        {
            var fields = new Dictionary<string, DataType>() {
                {"ID", DataType.String},
                {"DELETED", DataType.Boolean},
                {"CREATED_BY", DataType.String},
                {"DATE_ENTERED", DataType.DateTime},
                {"MODIFIED_USER_ID", DataType.String},
                {"DATE_MODIFIED", DataType.DateTime},
                {"DATE_MODIFIED_UTC", DataType.DateTime},
                {"ASSIGNED_USER_ID", DataType.String},
                {"TEAM_ID", DataType.String},
                {"NAME", DataType.String},
                {"DATE_START", DataType.DateTime},
                {"TIME_START", DataType.DateTime},
                {"PARENT_TYPE", DataType.String},
                {"PARENT_ID", DataType.String},
                {"DESCRIPTION", DataType.String},
                {"DESCRIPTION_HTML", DataType.String},
                {"FROM_ADDR", DataType.String},
                {"FROM_NAME", DataType.String},
            };

            var container = new DataContainer();
            var collection = new DataCollection(container, "emails-utf8.parquet");

            var resolver = new ParquetFileTypeResolver();
            var entities = resolver.ParseFileSchema(container, collection, "../../../tests/emails-utf8.parquet");

            foreach (var field in fields)
            {
                output.WriteLine($" check field {field.Key}");
                var entity = entities.Find(x => x.Name.Equals(field.Key));
                Assert.NotNull(entity);
                Assert.Equal(field.Value, entity.DataType);
            }
        }

        [Fact]
        public void TestParquetCollect()
        {
            var container = new DataContainer();
            var collection = new DataCollection(container, "emails-utf8.parquet");
            var entity = new DataEntity("FROM_ADDR", DataType.String, "String", container, collection);

            var resolver = new ParquetFileTypeResolver();
            var samples = resolver.CollectSamples(container, collection, entity, "../../../tests/emails-utf8.parquet", 5);
            Assert.Contains("sally@example.com", samples);
        }
    }
}
