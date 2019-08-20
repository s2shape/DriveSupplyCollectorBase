using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase
{
    public interface IFileProcessor {
        /// <summary>
        /// Returns true if this instance can process specified collection file
        /// </summary>
        bool CanProcess(string collectionName);

        /// <summary>
        /// Extract file schema
        /// </summary>
        List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, Stream fileStream, out long rowCount);

        List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity, int entityIndex, Stream fileStream, int maxSamples);
    }
}
