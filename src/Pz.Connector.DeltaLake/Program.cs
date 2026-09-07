using Pz.Connector.DeltaLake;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, new DeltaLakeConnector()).ConfigureAwait(false);
