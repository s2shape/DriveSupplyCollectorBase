using System;
using System.Collections.Generic;
using System.Linq;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace DriveSupplyCollectorBase
{
    public abstract class DriveSupplyCollectorBase : SupplyCollectorBase {
        private static IFileTypeResolver[] _resolvers = null;

        private IFileTypeResolver[] GetResolvers() {
            if (_resolvers == null) {
                var resolverInterfaceType = typeof(IFileTypeResolver);
                var resolverTypes = AppDomain.CurrentDomain
                    .GetAssemblies()
                    .SelectMany(s => s.GetTypes())
                    .Where(p => resolverInterfaceType.IsAssignableFrom(p));

                var resolvers = new List<IFileTypeResolver>();
                foreach (var resolverType in resolverTypes) {
                    var constructor = resolverType.GetConstructor(new Type[] { });
                    resolvers.Add((IFileTypeResolver) constructor.Invoke(new object[] { }));
                }

                _resolvers = resolvers.ToArray();
            }

            return _resolvers;
        }

        private IFileTypeResolver FindResolver(string collectionName) {
            var resolvers = GetResolvers();
            foreach (var resolver in resolvers) {
                if (resolver.CanProcess(collectionName))
                    return resolver;
            }

            return null;
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            
            throw new NotImplementedException();
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            throw new NotImplementedException();
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            throw new NotImplementedException();
        }
    }
}
