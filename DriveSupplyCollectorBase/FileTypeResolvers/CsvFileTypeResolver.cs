using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase.FileTypeResolvers
{
    public class CsvFileTypeResolver : IFileTypeResolver
    {
        public CsvFileTypeResolver() {
        }

        public bool CanProcess(string collectionName) {
            return Path.GetExtension(collectionName).Equals(".csv", StringComparison.InvariantCultureIgnoreCase);
        }

        public static Encoding GetEncoding(string filename)
        {
            // Read the BOM
            var bom = new byte[4];
            using (var file = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                file.Read(bom, 0, 4);
            }

            // Analyze the BOM
            if (bom[0] == 0x2b && bom[1] == 0x2f && bom[2] == 0x76) return Encoding.UTF7;
            if (bom[0] == 0xef && bom[1] == 0xbb && bom[2] == 0xbf) return Encoding.UTF8;
            if (bom[0] == 0xff && bom[1] == 0xfe) return Encoding.Unicode; //UTF-16LE
            if (bom[0] == 0xfe && bom[1] == 0xff) return Encoding.BigEndianUnicode; //UTF-16BE
            if (bom[0] == 0 && bom[1] == 0 && bom[2] == 0xfe && bom[3] == 0xff) return Encoding.UTF32;
            return Encoding.ASCII;
        }

        private DataType DetectDataType(string text) {
            var re_guid = new Regex(@"^\{[A-F0-9]{8}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{4}-[A-F0-9]{12}\}$");
            var re_double = new Regex(@"^-?\s*(\d\s?)+\.(\d\s?)+$");
            var re_int = new Regex(@"^-?\s*(\d\s?)+$");
            var re_date = new Regex(@"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+$");

            if (text.Equals("False", StringComparison.InvariantCultureIgnoreCase) ||
                text.Equals("True", StringComparison.InvariantCultureIgnoreCase))
                return DataType.Boolean;

            if (re_guid.IsMatch(text))
                return DataType.Guid;
            if (re_double.IsMatch(text))
                return DataType.Double;
            if (re_date.IsMatch(text))
                return DataType.DateTime;
            if (re_int.IsMatch(text))
                return DataType.Int;

            return DataType.String;
        }

        public List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, string fileName) {
            var entities = new List<DataEntity>();

            string header = null;
            string line0 = null;

            //var encoding = GetEncoding(fileName);
            using (var reader = new StreamReader(fileName, true)) {
                if (!reader.EndOfStream) {
                    header = reader.ReadLine();
                }
                if (!reader.EndOfStream) {
                    line0 = reader.ReadLine();
                }
            }

            if (!String.IsNullOrEmpty(header) && !String.IsNullOrEmpty(line0)) {
                var headerParts = header.Split(",");
                var lineParts = line0.Split(",");

                for (int i = 0; i < headerParts.Length && i < lineParts.Length; i++) {
                    var dataType = DetectDataType(lineParts[i].Trim());

                    entities.Add(new DataEntity(headerParts[i], dataType, Enum.GetName(typeof(DataType), dataType), container, collection));
                }
            }

            return entities;
        }

        public List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity,
            string fileName, int maxSamples) {
            var entities = ParseFileSchema(container, collection, fileName);
            var field = entities.Find(x => x.Name.Equals(entity.Name));
            var fieldInd = entities.IndexOf(field);

            var samples = new List<string>();
            using (var reader = new StreamReader(fileName, true)) {
                reader.ReadLine();

                while (!reader.EndOfStream && samples.Count < maxSamples) {
                    var line = reader.ReadLine();
                    if (String.IsNullOrEmpty(line))
                        continue;

                    var cells = line.Split(",");
                    if (cells.Length > fieldInd) {
                        samples.Add(cells[fieldInd].Trim());
                    }
                }
            }

            return samples;
        }
    }
}
