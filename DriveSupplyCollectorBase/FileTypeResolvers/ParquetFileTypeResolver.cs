using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Parquet;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase.FileTypeResolvers
{
    public class ParquetFileTypeResolver :IFileTypeResolver
    {
        public bool CanProcess(string collectionName) {
            return Path.GetExtension(collectionName).Equals(".parquet", StringComparison.InvariantCultureIgnoreCase);
        }

        private DataType ConvertDataType(Parquet.Data.DataType dataType) {
            switch (dataType) {
                case Parquet.Data.DataType.String:
                    return DataType.String;
                case Parquet.Data.DataType.Boolean:
                    return DataType.Boolean;
                case Parquet.Data.DataType.Int32:
                    return DataType.Int;
                case Parquet.Data.DataType.Double:
                    return DataType.Double;
                case Parquet.Data.DataType.DateTimeOffset:
                    return DataType.DateTime;
            }

            return DataType.Unknown;
        }

        public List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, string fileName) {
            var entities = new List<DataEntity>();

            using (var stream = new FileStream(fileName, FileMode.Open)) {
                var options = new ParquetOptions { TreatByteArrayAsString = true };
                var reader = new ParquetReader(stream, options);

                var schema = reader.Schema;
                var fields = schema.GetDataFields();
                foreach (var field in fields) {
                    entities.Add(new DataEntity(field.Name, ConvertDataType(field.DataType), Enum.GetName(typeof(Parquet.Data.DataType), field.DataType), container, collection));
                }
            }

            return entities;
        }

        public List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity, string fileName,
            int maxSamples) {
            var result = new List<string>();

            using (var stream = new FileStream(fileName, FileMode.Open))
            {
                var options = new ParquetOptions { TreatByteArrayAsString = true };
                var reader = new ParquetReader(stream, options);

                for (int i = 0; i < reader.RowGroupCount; i++) {
                    var columns = reader.ReadEntireRowGroup(i);

                    var column = columns.FirstOrDefault(x => x.Field.Name.Equals(entity.Name));
                    if (column != null) {
                        for (int j = 0; j < column.Data.Length; j++) {
                            result.Add(column.Data.GetValue(j)?.ToString());
                            if (result.Count >= maxSamples)
                                break;
                        }
                    }

                    if (result.Count >= maxSamples)
                        break;
                }
            }

            return result;
        }
    }
}
