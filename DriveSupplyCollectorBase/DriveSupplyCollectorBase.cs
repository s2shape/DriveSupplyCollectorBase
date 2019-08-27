using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase
{
    public abstract class DriveSupplyCollectorBase : SupplyCollectorBase {
        private static IFileProcessor[] _processors = null;

        /// <summary>
        /// Allows the user to specify a prefix which causes the supply collector to start in a sub-folder. Default is null.
        /// </summary>
        protected string s2Prefix;
        /// <summary>
        /// Allows the user to specify how many nested layers to include in the DataCollection name. Default is 0
        /// </summary>
        protected int s2FolderLevels = 0;
        /// <summary>
        /// Allows the user to specify whether or not to use the file name in the DataCollection name.  Default is false.
        /// </summary>
        protected bool s2UseFileNameInDcName = false;

        /// <summary>
        /// True if .csv files do not have header
        /// </summary>
        protected bool s2CsvNoHeader = false;

        private Dictionary<string, List<DriveFileInfo>> _fileDcMapping = null;
        private Dictionary<string, int> _fileEntityMapping = null;
        private List<DataCollection> _collections = null;
        private List<DataEntity> _entities = null;

        protected DriveSupplyCollectorBase(string s2Prefix, int s2FolderLevels, bool s2UseFileNameInDcName) {
            this.s2Prefix = s2Prefix;
            this.s2FolderLevels = s2FolderLevels;
            this.s2UseFileNameInDcName = s2UseFileNameInDcName;
        }

        protected void ParseConnectionStringAdditions(string additions) {
            var parts = additions.Split(",");
            foreach (var part in parts) {
                if(String.IsNullOrEmpty(part))
                    continue;

                var pair = part.Split("=");
                if (pair.Length == 2) {
                    if ("s2-prefix".Equals(pair[0])) {
                        s2Prefix = pair[1];
                    } else if ("s2-folder-levels-used-in-dc-name".Equals(pair[0])) {
                        s2FolderLevels = Int32.Parse(pair[1]);
                    } else if ("s2-use-file-name-in-dc-name".Equals(pair[0])) {
                        s2UseFileNameInDcName = Boolean.Parse(pair[1]);
                    } else if ("s2-csv-no-header".Equals(pair[0])) {
                        s2CsvNoHeader = Boolean.Parse(pair[1]);
                    }
                }
            }
        }

        private IFileProcessor[] GetProcessors() {
            if (_processors == null) {
                var processorInterfaceType = typeof(IFileProcessor);
                var processorTypes = AppDomain.CurrentDomain
                    .GetAssemblies()
                    .SelectMany(s => s.GetTypes())
                    .Where(p => processorInterfaceType.IsAssignableFrom(p));

                var args = new Dictionary<string, object> {{"csv-no-header", s2CsvNoHeader}};

                var processors = new List<IFileProcessor>();
                foreach (var processorType in processorTypes) {
                    if(!processorType.IsClass)
                        continue;

                    var argsConstructor = processorType.GetConstructor(new Type[] {typeof(Dictionary<string, object>)});
                    if (argsConstructor != null) {
                        var constructor = processorType.GetConstructor(new Type[] { typeof(Dictionary<string, object>) });
                        processors.Add((IFileProcessor)constructor.Invoke(new object[] { args }));
                    }
                    else {
                        var constructor = processorType.GetConstructor(new Type[] { });
                        processors.Add((IFileProcessor) constructor.Invoke(new object[] { }));
                    }
                }

                _processors = processors.ToArray();
            }

            return _processors;
        }

        protected IFileProcessor FindProcessor(string collectionName) {
            var processors = GetProcessors();
            foreach (var processor in processors) {
                if (processor.CanProcess(collectionName))
                    return processor;
            }

            return null;
        }

        protected abstract Stream GetFileStream(DataContainer container, string filePath);

        protected abstract List<DriveFileInfo> ListDriveFiles(DataContainer container);

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            if (_fileDcMapping == null || !_fileDcMapping.ContainsKey(dataEntity.Name)) {
                GetSchema(dataEntity.Container);
            }

            if (!_fileDcMapping.ContainsKey(dataEntity.Collection.Name)) {
                return new List<string>();
            }

            var samples = new List<string>();

            var files = _fileDcMapping[dataEntity.Collection.Name];
            foreach (var file in files) {
                var processor = FindProcessor(file.FilePath);
                if (processor == null)
                    continue;
                if(!_fileEntityMapping.ContainsKey($"{file.FilePath};{dataEntity.Name}"))
                    continue;

                int index = _fileEntityMapping[$"{file.FilePath};{dataEntity.Name}"];

                using (var stream = GetFileStream(dataEntity.Container, file.FilePath)) {
                    samples.AddRange(processor.CollectSamples(dataEntity.Container, dataEntity.Collection, dataEntity,
                        index,
                        stream, sampleSize));
                }
            }

            return samples;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            if (_fileDcMapping == null) {
                GetSchema(container);
            }

            var result = new List<DataCollectionMetrics>();
            foreach (var mappingPair in _fileDcMapping) {
                long size = 0;
                foreach (var fileInfo in mappingPair.Value) {
                    size += fileInfo.FileSize;
                }

                result.Add(new DataCollectionMetrics() {
                    Name = mappingPair.Key,
                    TotalSpaceKB = (decimal)size / 1024
                });
            }

            return result;
        }

        protected string BuildDataCollectionName(string path) {
            var parts = path.Split(new char[] {'/', '\\'});

            var result = new StringBuilder();

            for (int i = 0; i < parts.Length - 1 && i < s2FolderLevels; i++) {
                if(i > 0)
                    result.Append("/");
                result.Append(parts[i]);
            }

            if (s2UseFileNameInDcName || s2FolderLevels == 0) {
                if(result.Length>0)
                    result.Append("/");

                result.Append(parts[parts.Length - 1]);
            }

            return result.ToString();
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            _fileDcMapping = new Dictionary<string, List<DriveFileInfo>>();
            _fileEntityMapping = new Dictionary<string, int>();
            _collections = new List<DataCollection>();
            _entities = new List<DataEntity>();

            var files = ListDriveFiles(container);
            foreach (var file in files) {
                var processor = FindProcessor(file.FilePath);

                if (processor != null) {
                    var collectionName = BuildDataCollectionName(file.FilePath);

                    if (!_fileDcMapping.ContainsKey(collectionName)) {
                        _fileDcMapping.Add(collectionName, new List<DriveFileInfo>());
                    }
                    _fileDcMapping[collectionName].Add(file);

                    var collection = _collections.Find(x => x.Name.Equals(collectionName));
                    if (collection == null) {
                        collection = new DataCollection(container, collectionName);
                        _collections.Add(collection);
                    }

                    long rowCount;
                    var fileEntities =
                        processor.ParseFileSchema(container, collection, GetFileStream(container, file.FilePath), out rowCount);
                    file.RowCount = rowCount;

                    int index = 0;
                    foreach (var entity in fileEntities) {
                        _fileEntityMapping.Add($"{file.FilePath};{entity.Name}", index++);

                        if(!_entities.Exists(x => x.Name.Equals(entity.Name) && entity.Collection == x.Collection))
                            _entities.Add(entity);
                    }
                }
            }

            return (_collections, _entities);
        }
    }
}
