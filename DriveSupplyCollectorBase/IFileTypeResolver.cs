using System;
using System.Collections.Generic;
using System.Text;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase
{
    public interface IFileTypeResolver {
        /// <summary>
        /// Returns true if this instance can process specified collection file
        /// </summary>
        bool CanProcess(string collectionName);

        /// <summary>
        /// Extract file schema
        /// </summary>
        List<DataEntity> ParseFileSchema(DataContainer container, DataCollection collection, string fileName);

        List<string> CollectSamples(DataContainer container, DataCollection collection, DataEntity entity, string fileName, int maxSamples);
    }
}
