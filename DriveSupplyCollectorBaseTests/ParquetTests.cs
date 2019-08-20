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
            var processor = new ParquetFileProcessor();

            Assert.True(processor.CanProcess("emails-utf8.parquet"));
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

            var processor = new ParquetFileProcessor();
            List<DataEntity> entities;
            long rowCount = 0;
            using (var stream = File.Open("../../../tests/emails-utf8.parquet", FileMode.Open))
            {
                entities = processor.ParseFileSchema(container, collection, stream, out rowCount);
            }

            Assert.Equal(200, rowCount);

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

            var processor = new ParquetFileProcessor();

            long rowCount;
            List<DataEntity> entities;
            using (var stream = File.Open("../../../tests/emails-utf8.parquet", FileMode.Open))
            {
                entities = processor.ParseFileSchema(container, collection, stream, out rowCount);
            }

            var entity = entities.FirstOrDefault(x => x.Name.Equals("FROM_ADDR"));
            int index = entities.IndexOf(entity);

            using (var stream = File.Open("../../../tests/emails-utf8.parquet", FileMode.Open)) {
                var samples = processor.CollectSamples(container, collection, entity, index, stream, 5);
                Assert.Contains("sally@example.com", samples);
            }
        }
    }
}
