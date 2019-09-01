using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase.FileProcessors
{
    public class CsvFileProcessor : IFileProcessor {
        private bool csvHasHeader = true;

        public CsvFileProcessor() {
        }

        public CsvFileProcessor(Dictionary<string, object> args) {
            foreach (var arg in args) {
                if (arg.Key.Equals("csv_has_header")) {
                    csvHasHeader = (bool) arg.Value;
                }
            }
        }

        public bool CanProcess(string collectionName) {
            return Path.GetExtension(collectionName).Equals(".csv", StringComparison.InvariantCultureIgnoreCase);
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

        public List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, Stream fileStream, out long rowCount) {
            var entities = new List<DataEntity>();

            string header = null;
            string line0 = null;
            rowCount = 0;

            //var encoding = GetEncoding(fileName);
            using (var reader = new StreamReader(fileStream, true)) {
                if (csvHasHeader && !reader.EndOfStream) {
                    header = reader.ReadLine();
                }
                while (!reader.EndOfStream) {
                    rowCount++;

                    var line = reader.ReadLine();
                    if (line0 == null) {
                        line0 = line;
                    }
                }
            }

            if ((!csvHasHeader || !String.IsNullOrEmpty(header)) && !String.IsNullOrEmpty(line0)) {
                var lineParts = line0.Split(",");

                string[] headerParts;
                if (csvHasHeader) {
                    headerParts = header.Split(",");
                }
                else {
                    headerParts = new string[lineParts.Length];
                    for (int i = 0; i < headerParts.Length; i++)
                    {
                        headerParts[i] = $"column{i}";
                    }
                }

                for (int i = 0; i < headerParts.Length && i < lineParts.Length; i++) {
                    var dataType = DetectDataType(lineParts[i].Trim());

                    entities.Add(new DataEntity(headerParts[i], dataType, Enum.GetName(typeof(DataType), dataType), container, collection));
                }
            }

            return entities;
        }

        public List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity, int entityIndex, Stream fileStream, int maxSamples, double probability) {
            var rand = new Random();

            var samples = new List<string>();
            using (var reader = new StreamReader(fileStream, true)) {
                reader.ReadLine();

                while (!reader.EndOfStream && samples.Count < maxSamples) {
                    var line = reader.ReadLine();
                    if (String.IsNullOrEmpty(line))
                        continue;

                    if (rand.NextDouble() < probability) {
                        var cells = line.Split(",");
                        if (entityIndex < cells.Length) {
                            samples.Add(cells[entityIndex].Trim());
                        }
                    }
                }
            }

            return samples;
        }
    }
}
