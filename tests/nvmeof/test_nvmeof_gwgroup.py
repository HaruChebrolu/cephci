from copy import deepcopy

from ceph.ceph import Ceph
from ceph.nvmegw_cli import NVMeGWCLI
from ceph.nvmeof.initiator import Initiator
from ceph.parallel import parallel
from ceph.utils import get_node_by_id
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.nvme_utils import delete_nvme_service, deploy_nvme_service
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log
from utility.utils import generate_unique_id

LOG = Log(__name__)


def configure_listeners(ha, nodes, gw_group, config):
    """Configure Listeners on subsystem."""
    lb_group_ids = {}
    nqn = config["nqn"]
    if gw_group is not None and config.get("no-group-append", False) is False:
        nqn = f"{config['nqn']}.{gw_group}"
    for node in nodes:
        nvmegwcli = ha.check_gateway(node)
        hostname = nvmegwcli.fetch_gateway_hostname()
        listener_config = {
            "args": {
                "subsystem": nqn,
                "traddr": nvmegwcli.node.ip_address,
                "trsvcid": config["listener_port"],
                "host-name": hostname,
            }
        }
        nvmegwcli.listener.add(**listener_config)
        lb_group_ids.update({hostname: nvmegwcli.ana_group_id})
    return lb_group_ids


def configure_subsystems(pool, ha, gw_group, config):
    """Configure Ceph-NVMEoF Subsystems."""
    nqn = config["nqn"]
    if gw_group is not None and config.get("no-group-append", False) is False:
        nqn = f"{config['nqn']}.{gw_group}"
    sub_args = {"subsystem": config["nqn"]}
    ceph_cluster = config["ceph_cluster"]

    nvmegwcli = ha.gateways[0]

    # Add Subsystem
    nvmegwcli.subsystem.add(
        **{
            "args": {
                **sub_args,
                **{
                    "max-namespaces": config.get("max_ns", 32),
                    "enable-ha": config.get("enable_ha", False),
                    "no-group-append": config.get("no-group-append", False),
                },
            }
        }
    )

    sub_args["subsystem"] = nqn
    # Add Listeners
    listeners = [nvmegwcli.node.hostname]
    if config.get("listeners"):
        listeners = config["listeners"]
    lb_groups = configure_listeners(ha, listeners, gw_group, config)

    # Add Host access
    nvmegwcli.host.add(**{"args": {**sub_args, **{"host": repr(config["allow_host"])}}})

    # Add Namespaces
    if config.get("bdevs"):
        bdev_configs = config["bdevs"]
        if isinstance(config["bdevs"], dict):
            bdev_configs = [config["bdevs"]]
        for bdev_cfg in bdev_configs:
            name = generate_unique_id(length=4)
            namespace_args = {
                **sub_args,
                **{
                    "rbd-pool": pool,
                    "rbd-create-image": True,
                    "size": bdev_cfg["size"],
                },
            }
            with parallel() as p:
                for num in range(bdev_cfg["count"]):
                    ns_args = deepcopy(namespace_args)
                    ns_args["rbd-image"] = f"{name}-image{num}"
                    if bdev_cfg.get("lb_group"):
                        lbgid = lb_groups[
                            get_node_by_id(ceph_cluster, bdev_cfg["lb_group"]).hostname
                        ]
                        ns_args["load-balancing-group"] = lbgid
                    ns_args = {"args": ns_args}
                    p.spawn(nvmegwcli.namespace.add, **ns_args)


def disconnect_initiator(ceph_cluster, node):
    """Disconnect Initiator."""
    node = get_node_by_id(ceph_cluster, node)
    initiator = Initiator(node)
    initiator.disconnect_all()


def teardown(ceph_cluster, rbd_obj, config):
    """Cleanup the ceph-nvme gw entities.

    Args:
        ceph_cluster: Ceph Cluster
        rbd_obj: RBD object
        config: test config
    """

    # Delete the gateway
    if "gateway" in config["cleanup"]:
        delete_nvme_service(ceph_cluster, config)

    for gwgroup_config in config["gw_groups"]:
        if "initiators" in config["cleanup"]:
            for initiator_cfg in gwgroup_config["initiators"]:
                disconnect_initiator(ceph_cluster, initiator_cfg["node"])

    # Delete the pool
    if "pool" in config["cleanup"]:
        rbd_obj.clean_up(pools=[config["rbd_pool"]])


def run(ceph_cluster: Ceph, **kwargs) -> int:

    LOG.info("Starting Ceph NVMEoF deployment.")
    config = kwargs["config"]
    rbd_pool = config["rbd_pool"]
    rbd_obj = initial_rbd_config(**kwargs)["rbd_reppool"]

    overrides = kwargs.get("test_data", {}).get("custom-config")
    for key, value in dict(item.split("=") for item in overrides).items():
        if key == "nvmeof_cli_image":
            NVMeGWCLI.NVMEOF_CLI_IMAGE = value
            break

    try:
        for gwgroup_config in config["gw_groups"]:
            gwgroup_config["rbd_pool"] = rbd_pool

            # Deploy NVMeOf services
            if config.get("install"):
                deploy_nvme_service(ceph_cluster, gwgroup_config)

            ha = HighAvailability(
                ceph_cluster, gwgroup_config["gw_nodes"], **gwgroup_config
            )

            # Configure subsystems in GWgroups
            if gwgroup_config.get("subsystems"):
                with parallel() as p:
                    for subsys_args in gwgroup_config["subsystems"]:
                        gw_group = gwgroup_config.get("gw_group", None)
                        subsys_args["ceph_cluster"] = ceph_cluster
                        p.spawn(
                            configure_subsystems, rbd_pool, ha, gw_group, subsys_args
                        )

            # HA failover and failback
            if gwgroup_config.get("fault-injection-methods") or config.get(
                "fault-injection-methods"
            ):
                ha.run()
            if "initiators" in config["cleanup"]:
                for initiator_cfg in gwgroup_config["initiators"]:
                    disconnect_initiator(ceph_cluster, initiator_cfg["node"])

        return 0
    except Exception as err:
        LOG.error(err)
    finally:
        if config.get("cleanup"):
            teardown(ceph_cluster, rbd_obj, config)

    return 1
