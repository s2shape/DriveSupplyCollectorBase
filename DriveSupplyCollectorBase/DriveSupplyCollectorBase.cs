using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

        protected DriveSupplyCollectorBase(string s2Prefix, int s2FolderLevels, bool s2UseFileNameInDcName) {
            this.s2Prefix = s2Prefix;
            this.s2FolderLevels = s2FolderLevels;
            this.s2UseFileNameInDcName = s2UseFileNameInDcName;
        }


        private IFileProcessor[] GetProcessors() {
            if (_processors == null) {
                var processorInterfaceType = typeof(IFileProcessor);
                var processorTypes = AppDomain.CurrentDomain
                    .GetAssemblies()
                    .SelectMany(s => s.GetTypes())
                    .Where(p => processorInterfaceType.IsAssignableFrom(p));

                var processors = new List<IFileProcessor>();
                foreach (var processorType in processorTypes) {
                    if(!processorType.IsClass)
                        continue;
                    
                    var constructor = processorType.GetConstructor(new Type[] { });
                    processors.Add((IFileProcessor) constructor.Invoke(new object[] { }));
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
            var processor = FindProcessor(dataEntity.Collection.Name);
            if (processor == null) {
                return new List<string>();
            }

            List<DataEntity> entities;
            using (var stream = GetFileStream(dataEntity.Container, dataEntity.Collection.Name)) {
                entities = processor.ParseFileSchema(dataEntity.Container, dataEntity.Collection, stream);
            }

            int index = entities.IndexOf(entities.Find(x => x.Name.Equals(dataEntity.Name)));
            if (index < 0)
                throw new ArgumentException("Entity not found in schema!");

            using (var stream = GetFileStream(dataEntity.Container, dataEntity.Collection.Name)) {
                return processor.CollectSamples(dataEntity.Container, dataEntity.Collection, dataEntity, index,
                    stream, sampleSize);
            }
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            throw new NotImplementedException();
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            var files = ListDriveFiles(container);
            foreach (var file in files) {
                var processor = FindProcessor(file.FilePath);

                if (processor != null) {
                    var collection = new DataCollection(container, file.FilePath);

                    entities.AddRange(processor.ParseFileSchema(container, collection, GetFileStream(container, file.FilePath)));

                    collections.Add(collection);
                }
            }

            return (collections, entities);
        }
    }
}
