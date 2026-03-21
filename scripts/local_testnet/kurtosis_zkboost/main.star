# Kurtosis package that runs the ethereum-package and then adds zkboost-server
# sidecar services for real proof generation.
#
# Usage:
#   kurtosis run --enclave eip8025-zkboost ./kurtosis_zkboost \
#       --args-file network_params_eip8025_zkboost.yaml
#
# The args file must include a top-level `zkboost` key alongside standard
# ethereum-package configuration.  Example:
#
#   zkboost:
#     image: ghcr.io/eth-act/zkboost/zkboost-server:1715344
#     instances:
#       - name: zkboost-1
#         el_service: el-1-geth-lighthouse
#       - name: zkboost-2
#         el_service: el-2-geth-lighthouse
#     mock_proving_time_ms: 5000
#     mock_proof_size: 1024

ethereum_package = import_module("github.com/ethpandaops/ethereum-package/main.star")

ZKBOOST_PORT_ID = "http"
ZKBOOST_PORT_NUMBER = 3000
ZKBOOST_METRICS_PATH = "/metrics"

# Default mock zkVM config — real proving backends can be configured via
# external ere-server instances if needed.
ZKBOOST_CONFIG_TEMPLATE = """\
port = {port}
el_endpoint = "http://{el_service}:{el_rpc_port}"
chain_config_path = "/app/chain_config.json"

[[zkvm]]
kind = "mock"
mock_proving_time_ms = {mock_proving_time_ms}
mock_proof_size = {mock_proof_size}
proof_type = "reth-zisk"
"""

# Chain config matching the Kurtosis devnet genesis (chainId 3151908).
# Providing this as a file avoids the debug_chainConfig RPC call, which
# is not available in standard geth.
CHAIN_CONFIG_JSON = """\
{
  "chainId": 3151908,
  "homesteadBlock": 0,
  "daoForkSupport": false,
  "eip150Block": 0,
  "eip155Block": 0,
  "eip158Block": 0,
  "byzantiumBlock": 0,
  "constantinopleBlock": 0,
  "petersburgBlock": 0,
  "istanbulBlock": 0,
  "berlinBlock": 0,
  "londonBlock": 0,
  "mergeNetsplitBlock": 0,
  "shanghaiTime": 0,
  "cancunTime": 0,
  "pragueTime": 0,
  "osakaTime": 0,
  "bpo1Time": 0,
  "terminalTotalDifficulty": 0,
  "terminalTotalDifficultyPassed": true,
  "depositContractAddress": "0x00000000219ab540356cbb839cbe05303d7705fa",
  "blobSchedule": {
    "bpo1": { "baseFeeUpdateFraction": 8346193, "max": 15, "target": 10 },
    "cancun": { "baseFeeUpdateFraction": 3338477, "max": 6, "target": 3 },
    "osaka": { "baseFeeUpdateFraction": 5007716, "max": 9, "target": 6 },
    "prague": { "baseFeeUpdateFraction": 5007716, "max": 9, "target": 6 }
  }
}
"""

def run(plan, args):
    """Start ethereum-package then add zkboost-server sidecars."""

    # Split out zkboost config from ethereum-package args.
    zkboost_args = args.pop("zkboost", None)
    if zkboost_args == None:
        fail("Missing required 'zkboost' key in args file.")

    # Run the standard ethereum-package with the remaining args.
    ethereum_package.run(plan, args)

    # Extract zkboost settings with defaults.
    zkboost_image = zkboost_args.get("image", "ghcr.io/eth-act/zkboost/zkboost-server:1715344")
    instances = zkboost_args.get("instances", [])
    mock_proving_time_ms = zkboost_args.get("mock_proving_time_ms", 5000)
    mock_proof_size = zkboost_args.get("mock_proof_size", 1024)
    el_rpc_port = zkboost_args.get("el_rpc_port", 8545)

    if len(instances) == 0:
        fail("zkboost.instances must contain at least one entry.")

    for instance in instances:
        name = instance["name"]
        el_service = instance["el_service"]

        config_content = ZKBOOST_CONFIG_TEMPLATE.format(
            port = ZKBOOST_PORT_NUMBER,
            el_service = el_service,
            el_rpc_port = el_rpc_port,
            mock_proving_time_ms = mock_proving_time_ms,
            mock_proof_size = mock_proof_size,
        )

        config_artifact = plan.render_templates(
            name = name + "-config",
            config = {
                "config.toml": struct(
                    template = config_content,
                    data = {},
                ),
                "chain_config.json": struct(
                    template = CHAIN_CONFIG_JSON,
                    data = {},
                ),
            },
        )

        plan.add_service(
            name = name,
            config = ServiceConfig(
                image = zkboost_image,
                cmd = ["--config", "/app/config.toml"],
                ports = {
                    ZKBOOST_PORT_ID: PortSpec(
                        number = ZKBOOST_PORT_NUMBER,
                        transport_protocol = "TCP",
                        application_protocol = "http",
                    ),
                },
                files = {
                    "/app": config_artifact,
                },
                env_vars = {
                    "RUST_LOG": "info,zkboost=debug",
                },
            ),
        )

        plan.print("Started zkboost service '{0}' -> EL '{1}'".format(name, el_service))
