using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase.FileTypeResolvers
{
    public class JsonFileTypeResolver : IFileTypeResolver
    {
        public bool CanProcess(string collectionName) {
            return Path.GetExtension(collectionName).Equals(".json", StringComparison.InvariantCultureIgnoreCase);
        }

        private void FillObjectEntities(DataContainer container, DataCollection collection, string prefix, JObject obj, List<DataEntity> entities) {
            var properties = obj.Properties();
            foreach (var property in properties) {
                switch (property.Value.Type) {
                    case JTokenType.Boolean:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Boolean, "Boolean", container, collection));
                        break;
                    case JTokenType.String:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.String, "String", container, collection));
                        break;
                    case JTokenType.Date:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.DateTime, "Date", container, collection));
                        break;
                    case JTokenType.Float:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Float, "Float", container, collection));
                        break;
                    case JTokenType.Integer:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Int, "Integer", container, collection));
                        break;
                    case JTokenType.Guid:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.Guid, "Guid", container, collection));
                        break;
                    case JTokenType.Uri:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.String, "Uri", container, collection));
                        break;
                    case JTokenType.Array:
                        entities.Add(new DataEntity($"{prefix}{property.Name}", DataType.String, "Array", container, collection));
                        break;
                    case JTokenType.Object:
                        FillObjectEntities(container, collection, $"{property.Name}.", (JObject)property.Value, entities);
                        break;
                    default:
                        entities.Add(new DataEntity($"{prefix}{property.Name}",
                            DataType.Unknown, Enum.GetName(typeof(JTokenType), property.Value.Type), container, collection));
                        break;
                }
            }
        }

        public List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, string fileName) {
            var entities = new List<DataEntity>();

            var serializer = new JsonSerializer();

            using (var fileStream = File.OpenText(fileName)) {
                using (var jsonReader = new JsonTextReader(fileStream)) {
                    var root = serializer.Deserialize(jsonReader);

                    if (root is JArray) {
                        var obj = (JObject)((JArray) root).First;

                        if (obj != null) {
                            FillObjectEntities(container, collection, "", obj, entities);
                        }
                    }
                    else {
                        throw new ArgumentException("Array expected!");
                    }
                }
            }

            return entities;
        }

        public List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity, string fileName, int maxSamples) {
            var samples = new List<string>();
            var serializer = new JsonSerializer();

            using (var fileStream = File.OpenText(fileName))
            {
                using (var jsonReader = new JsonTextReader(fileStream))
                {
                    var root = serializer.Deserialize(jsonReader);

                    if (root is JArray) {
                        var arr = ((JArray) root);

                        for (int j = 0; j < arr.Count && j < maxSamples; j++) {
                            var obj = (JObject) arr[j];

                            if (obj != null) {
                                var propertyPath = entity.Name;
                                var properties = propertyPath.Split(".");

                                for (int i = 0; i < properties.Length; i++) {
                                    if (i == properties.Length - 1) {
                                        var token = obj[properties[i]];
                                        if (token == null) {
                                            samples.Add(null);
                                        }
                                        else {
                                            samples.Add(token.ToString());
                                        }
                                    }
                                    else {
                                        obj = obj[properties[i]] as JObject;

                                        if (obj == null) {
                                            samples.Add(null);
                                            break;
                                        }
                                    }
                                }
                            }
                            else {
                                samples.Add(null);
                            }
                        }
                    }
                    else
                    {
                        throw new ArgumentException("Array expected!");
                    }
                }
            }
            return samples;
        }
    }
}
