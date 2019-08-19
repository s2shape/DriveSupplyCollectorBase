using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase.FileProcessors
{
    public class JsonFileProcessor : IFileProcessor
    {
        public bool CanProcess(string collectionName) {
            return Path.GetExtension(collectionName).Equals(".json", StringComparison.InvariantCultureIgnoreCase);
        }

        private void FillObjectEntities(DataContainer container, DataCollection collection, string prefix, JObject obj, List<DataEntity> entities) {
            var properties = obj.Properties();
            foreach (var property in properties) {
                if (entities.Find(x => x.Name.Equals($"{prefix}{property.Name}")) != null) {
                    continue;
                }

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

                        var arr = (JArray) property.Value;
                        for (int i = 0; i < arr.Count; i++) {
                            var arrayItem = arr[i];

                            if (arrayItem.Type == JTokenType.Object) {
                                FillObjectEntities(container, collection, $"{prefix}{property.Name}.", (JObject)arrayItem, entities);
                            }
                        }
                        
                        break;
                    case JTokenType.Object:
                        FillObjectEntities(container, collection, $"{prefix}{property.Name}.", (JObject)property.Value, entities);
                        break;
                    default:
                        entities.Add(new DataEntity($"{prefix}{property.Name}",
                            DataType.Unknown, Enum.GetName(typeof(JTokenType), property.Value.Type), container, collection));
                        break;
                }
            }
        }

        public List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, Stream fileStream) {
            var entities = new List<DataEntity>();

            var serializer = new JsonSerializer();

            using (var reader = new StreamReader(fileStream)) {
                using (var jsonReader = new JsonTextReader(reader)) {
                    var root = serializer.Deserialize(jsonReader);

                    if (root is JArray) {
                        var arr = (JArray) root;

                        for(int i=0;i<arr.Count;i++) {
                            if (arr[i].Type == JTokenType.Object) {
                                FillObjectEntities(container, collection, "", (JObject)arr[i], entities);
                            }
                        }
                    } else if (root is JObject) {
                        FillObjectEntities(container, collection, "", (JObject)root, entities);
                    }
                    else {
                        throw new ArgumentException("Array or object expected!");
                    }
                }
            }

            return entities;
        }

        private void FillObjectSamples(DataEntity entity, string prefix, JObject obj, List<string> samples) {
            var properties = obj.Properties();
            foreach (var property in properties) {
                if (!entity.Name.StartsWith($"{prefix}{property.Name}"))
                    continue;

                if (entity.Name.Equals($"{prefix}{property.Name}")) {
                    if (property.Value.Type == JTokenType.Array) {
                        var arr = (JArray) property.Value;
                        foreach (var item in arr) {
                            samples.Add(item.ToString());
                        }
                    }
                    else {
                        samples.Add(property.Value.ToString());
                    }
                } else if (property.Value.Type == JTokenType.Array) {
                    var arr = (JArray)property.Value;
                    foreach(var arrayItem in arr)
                    {
                        if (arrayItem.Type == JTokenType.Object)
                        {
                            FillObjectSamples(entity, $"{prefix}{property.Name}.", (JObject)arrayItem, samples);
                        }
                    }
                }
                else if (property.Value.Type == JTokenType.Object) {
                    FillObjectSamples(entity, $"{prefix}{property.Name}.", (JObject)property.Value, samples);
                }
            }
        }

        public List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity, int entityIndex, Stream fileStream, int maxSamples) {
            var samples = new List<string>();
            var serializer = new JsonSerializer();

            using (var reader = new StreamReader(fileStream))
            {
                using (var jsonReader = new JsonTextReader(reader))
                {
                    var root = serializer.Deserialize(jsonReader);

                    if (root is JArray)
                    {
                        var arr = (JArray)root;

                        for (int i = 0; i < arr.Count; i++)
                        {
                            if (arr[i].Type == JTokenType.Object)
                            {
                                FillObjectSamples(entity, "", (JObject)arr[i], samples);
                            }
                        }
                    }
                    else if (root is JObject)
                    {
                        FillObjectSamples(entity, "", (JObject)root, samples);
                    }
                    else
                    {
                        throw new ArgumentException("Array or object expected!");
                    }
                }
            }
            return samples;
        }
    }
}
