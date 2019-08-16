using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CsvConverter
{
    class Program {
        private static Dictionary<string, string> json_mapping= new Dictionary<string, string>() {
                {"ID", "id"},
                {"DELETED", "deleted"},
                {"CREATED_BY", "created.user"},
                {"DATE_ENTERED", "created.date"},
                {"MODIFIED_USER_ID", "modified.user"},
                {"DATE_MODIFIED", "modified.date"},
                {"DATE_MODIFIED_UTC", "modified.date_utc"},
                {"ASSIGNED_USER_ID", "assigned_user_id"},
                {"TEAM_ID", "team_id"},
                {"NAME", "name"},
                {"DATE_START", "start.date"},
                {"TIME_START", "start.time"},
                {"PARENT_TYPE", "parent.type"},
                {"PARENT_ID", "parent.id"},
                {"DESCRIPTION", "description.text"},
                {"DESCRIPTION_HTML", "description.html"},
                {"FROM_ADDR", "from.addr"},
                {"FROM_NAME", "from.name"},
                {"TO_ADDRS", "to_addrs"},
                {"CC_ADDRS", "cc_addrs"},
                {"BCC_ADDRS", "bcc_addrs"},
                {"TYPE", "type"},
                {"STATUS", "status"},
                {"MESSAGE_ID", "message_id"},
                {"REPLY_TO_NAME", "reply_to.name"},
                {"REPLY_TO_ADDR", "reply_to.addr"},
                {"INTENT", "intent"},
                {"MAILBOX_ID", "mailbox_id"},
                {"RAW_SOURCE", "raw_source"},
                {"TEAM_SET_ID", "team_set_id"}
        };

        private static Dictionary<string, JTokenType> json_types = new Dictionary<string, JTokenType>() {
            {"ID", JTokenType.Guid},
            {"DELETED", JTokenType.Boolean},
            {"CREATED_BY", JTokenType.Guid},
            {"DATE_ENTERED", JTokenType.Date},
            {"MODIFIED_USER_ID", JTokenType.Guid},
            {"DATE_MODIFIED", JTokenType.Date},
            {"DATE_MODIFIED_UTC", JTokenType.Date},
            {"ASSIGNED_USER_ID", JTokenType.Guid},
            {"TEAM_ID", JTokenType.Guid},
            {"NAME", JTokenType.String},
            {"DATE_START", JTokenType.Date},
            {"TIME_START", JTokenType.Date},
            {"PARENT_TYPE", JTokenType.String},
            {"PARENT_ID", JTokenType.Guid},
            {"DESCRIPTION", JTokenType.String},
            {"DESCRIPTION_HTML", JTokenType.String},
            {"FROM_ADDR", JTokenType.String},
            {"FROM_NAME", JTokenType.String},
            {"TO_ADDRS", JTokenType.String},
            {"CC_ADDRS", JTokenType.String},
            {"BCC_ADDRS", JTokenType.String},
            {"TYPE", JTokenType.String},
            {"STATUS", JTokenType.String},
            {"MESSAGE_ID", JTokenType.Guid},
            {"REPLY_TO_NAME", JTokenType.String},
            {"REPLY_TO_ADDR", JTokenType.String},
            {"INTENT", JTokenType.String},
            {"MAILBOX_ID", JTokenType.Guid},
            {"RAW_SOURCE", JTokenType.String},
            {"TEAM_SET_ID", JTokenType.Guid}
        };

        private static JValue ParseValue(JTokenType type, string value) {
            if (String.IsNullOrEmpty(value))
                return new JValue((string)null);

            switch (type) {
                case JTokenType.Boolean:
                    return new JValue(Boolean.Parse(value));
                case JTokenType.Guid:
                    return new JValue(Guid.Parse(value));
                case JTokenType.Integer:
                    return new JValue(Int32.Parse(value));
                case JTokenType.Float:
                    return new JValue(Double.Parse(value));
                case JTokenType.Date:
                    return new JValue(DateTime.Parse(value));
            }
            return new JValue(value);
        }

        static void ConvertCsvToJson(string inputFile, string outputFile) {
            var jarr = new JArray();
            using (var reader = new StreamReader(inputFile, true)) {
                var header = reader.ReadLine();

                var columns = header.Split(",");
                for (int i = 0; i < columns.Length; i++) {
                    columns[i] = columns[i].Trim();
                }

                while (!reader.EndOfStream) {
                    var line = reader.ReadLine();
                    if(String.IsNullOrEmpty(line))
                        continue;

                    var root = new JObject();

                    var parts = line.Split(",");
                    for (int i = 0; i < parts.Length && i < columns.Length; i++) {
                        var column = columns[i];

                        if (json_mapping.ContainsKey(column)) {
                            var path = json_mapping[column];

                            var path_parts = path.Split(".");
                            var curObj = root;

                            for (int j = 0; j < path_parts.Length - 1; j++) {
                                var subProperty = path_parts[j];
                                if (!curObj.ContainsKey(subProperty)) {
                                    curObj.Add(subProperty, new JObject());
                                }
                                curObj = (JObject) curObj[subProperty];
                            }

                            curObj.Add(path_parts[path_parts.Length - 1], ParseValue(json_types[column], parts[i]));
                        }
                    }

                    
                    jarr.Add(root);
                }
            }

            var json = JsonConvert.SerializeObject(jarr);
            File.WriteAllText(outputFile, json);
        }

        static void ConvertCsvToParquet(string inputFile, string outputFile) {

        }


        static void Main(string[] args) {
            if (args.Length < 2) {
                Console.WriteLine("Usage: CsvConverter <input> <output>");
                return;
            }

            var inputFile = args[0];
            var outputFile = args[1];

            if (!".csv".Equals(Path.GetExtension(inputFile), StringComparison.InvariantCultureIgnoreCase)) {
                Console.WriteLine("Can only convert .csv files!");
                return;
            }

            if (".json".Equals(Path.GetExtension(outputFile), StringComparison.InvariantCultureIgnoreCase)) {
                ConvertCsvToJson(inputFile, outputFile);
            }
            else if (".parquet".Equals(Path.GetExtension(outputFile), StringComparison.InvariantCultureIgnoreCase)) {
                ConvertCsvToParquet(inputFile, outputFile);
            }
            else {
                Console.WriteLine("Unsupported output file format!");
            }
        }
    }
}
