using Pz.Connector.DeltaLake;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, _ => new DeltaLakeConnector()).ConfigureAwait(false);
