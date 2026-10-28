using System.Collections.Concurrent;
using Azure.Core;
using Azure.Identity;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DeltaLake;

/// <summary>A storage access token for the DuckDB half of an Azure identity read. DuckDB's delta extension
/// hands delta-kernel credentials only for an <c>access_token</c>, <c>service_principal</c>, connection
/// string or <c>credential_chain</c> secret (duckdb-delta <c>CreateBuilder</c>). A <c>managed_identity</c>
/// secret falls through to <c>azure_skip_signature</c>, so the log read goes out unauthenticated (measured on
/// Azure Container Apps: <c>401 NoAuthenticationInformation</c> on <c>_delta_log/_last_checkpoint</c>), and
/// <c>credential_chain</c> asks the VM metadata endpoint with no client id. The connector therefore gets the
/// token itself and gives DuckDB an <c>access_token</c> secret, which both delta-kernel and the azure
/// extension honour.
///
/// <para>With a client id it is that user-assigned managed identity; without one it is the default chain
/// (environment service principal, workload identity, managed identity via <c>AZURE_CLIENT_ID</c>, then
/// developer sign-ins such as <c>az login</c>). Both read the identity variables pz 0.14+ forwards to this
/// package (<c>PzAmbientIdentity</c>). Credentials are kept per client id so repeated scans reuse
/// Azure.Identity's own token cache. The token never reaches a message: failures name the cause only.</para></summary>
internal static class DeltaAzureToken
{
    private static readonly TokenRequestContext Storage = new(["https://storage.azure.com/.default"]);

    private static readonly ConcurrentDictionary<string, TokenCredential> Credentials = new(StringComparer.Ordinal);

    public static string Fetch(string? clientId)
    {
        var credential = Credentials.GetOrAdd(clientId ?? string.Empty, static id => id.Length > 0
            ? new ManagedIdentityCredential(ManagedIdentityId.FromUserAssignedClientId(id))
            : new DefaultAzureCredential());
        try
        {
            return credential.GetToken(Storage, CancellationToken.None).Token;
        }
        catch (AuthenticationFailedException ex)
        {
            var who = string.IsNullOrEmpty(clientId)
                ? "the default Azure credential chain"
                : $"the user-assigned managed identity with client id '{clientId}'";
            throw new PzConnectorException(
                $"{DeltaErrors.AzureTokenUnavailable}: no Azure storage token from {who} " +
                $"({ex.GetType().Name}: {ex.Message.Split('\n')[0].Trim()}). Next step: check that this host has " +
                "that identity and that pz is 0.14.0 or newer, so the identity variables reach the connector",
                isTransient: false);
        }
    }
}
