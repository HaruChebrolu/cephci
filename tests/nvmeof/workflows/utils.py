import json
from utility.log import Log
from utility.retry import retry
from utility.utils import log_json_dump

LOG = Log(__name__)


def get_optimized_state(self, failed_ana_id):
    """Fetch the Optimized ANA states for failed gateway.

    Args:
        gateway: The gateway which is operational.
        failed_ana_id: failed gateway ANA Group Id.

    Returns:
        gateways which shows ACTIVE state for failed ANA Group Id
    """
    # get ANA states
    ana_states = self.ana_states()

    # Fetch failed ANA Group Id in ACTIVE state
    found = []

    for ana_gw_id, state in ana_states.items():
        if (
            state["Availability"] == "AVAILABLE"
            and state.get(failed_ana_id) == "ACTIVE"
        ):
            found.append({ana_gw_id: state})

    return found


def check_gateway_availability(self, ana_id, state="AVAILABLE", ana_states=None):
    """Check for failed ANA GW become unavailable.

    Args:
        ana_id: Gateway ANA group id.
        state: Gateway availability state
        ana_states: Overall ana state. (output from self.ana_states)
    Return:
        True if Gateway availability is in expected state, else False
    """
    # get ANA states
    if not ana_states:
        ana_states = get_ana_states()

    # Check Availability of ANA Group Gateway
    for _, _state in ana_states.items():
        if _state["anagrp-id"] == ana_id:
            if _state["Availability"] == state:
                return True
            return False
    return False


def catogorize(self, gws):
    """Categorize to-be failed and running GWs.

    Args:
        gws: gateways to be failed/stopped/scaled-down

    Returns:
        list of,
            - to-be failed gateways
            - rest of the gateways
    """
    fail_gws = []
    running_gws = []

    # collect impending Gateways to be failed.
    if isinstance(gws, str):
        gws = [gws]
    for gw_id in gws:
        fail_gws.append(self.check_gateway(gw_id))

    # Collect rest of the Gateways
    for gw in self.gateways:
        if gw.node.id not in gws:
            running_gws.append(gw)

    return fail_gws, running_gws


@retry(IOError, tries=3, delay=3)
def compare_client_namespace(self, uuids):
    lsblk_devs = []
    for client in self.clients:
        lsblk_devs.extend(client.fetch_lsblk_nvme_devices())

    LOG.info(
        f"Expected NVMe Targets : {set(list(uuids))} Vs LSBLK devices: {set(list(lsblk_devs))}"
    )
    if sorted(uuids) != sorted(set(lsblk_devs)):
        raise IOError("Few Namespaces are missing!!!")
    LOG.info("All namespaces are listed at Client(s)")
    return True


def fetch_namespaces(self, gateway, failed_ana_grp_ids=[], get_list=False):
    """Fetch all namespaces for failed gateways.

    Args:
        gateway: Operational gateway
        failed_ana_grp_ids: Failed or to-be failed gateway ids
    Returns:
        list of namespaces
    """
    args = {"base_cmd_args": {"format": "json"}}
    _, subsystems = gateway.subsystem.list(**args)
    subsystems = json.loads(subsystems)

    namespaces = []
    all_ns = []
    for subsystem in subsystems["subsystems"]:
        sub_name = subsystem["nqn"]
        cmd_args = {"args": {"subsystem": subsystem["nqn"]}}
        _, nspaces = gateway.namespace.list(**{**args, **cmd_args})
        nspaces = json.loads(nspaces)["namespaces"]
        all_ns.extend(nspaces)

        if failed_ana_grp_ids:
            for ns in nspaces:
                if ns["load_balancing_group"] in failed_ana_grp_ids:
                    # <subsystem>|<nsid>|<pool_name>|<image>
                    ns_info = f"nsid-{ns['nsid']}|{ns['rbd_pool_name']}|{ns['rbd_image_name']}"
                    if get_list:
                        namespaces.append({"list": ns, "info": f"{sub_name}|{ns_info}"})
                    else:
                        namespaces.append(f"{sub_name}|{ns_info}")
    if not failed_ana_grp_ids:
        LOG.info(f"All namespaces : {log_json_dump(all_ns)}")
        return all_ns

    LOG.info(
        f"Namespaces found for ANA-grp-id [{failed_ana_grp_ids}]: {log_json_dump(namespaces)}"
    )
    return namespaces


def get_ana_states(self, gw_group=""):
    """Fetch ANA states and convert into python dict."""

    out, _ = self.orch.shell(
        args=["ceph", "nvme-gw", "show", self.nvme_pool, repr(self.gateway_group)]
    )
    states = {}
    if self.cluster.rhcs_version >= "8":
        out = json.loads(out)
        for gateway in out.get("Created Gateways:"):
            gw = gateway["gw-id"]
            states[gw] = gateway
            states[gw].update(self.string_to_dict(gateway["ana states"]))
    else:
        for data in out.split("}"):
            data = data.strip()
            if not data:
                continue
            data = json.loads(f"{data}}}")
            if data.get("ana states"):
                gw = data["gw-id"]
                states[gw] = data
                states[gw].update(self.string_to_dict(data["ana states"]))

    return states


def check_gateway(self, node_id):
    """Check node is NVMeoF Gateway node.

    Args:
        node_id: Ceph node Id (ex., node6)
    """
    for gw in self.gateways:
        if gw.node.id == node_id:
            LOG.info(f"[{node_id}] {gw.node.hostname} is NVMeoF Gateway node.")
            return gw
    raise Exception(f"{node_id} doesn't match to any gateways provided...")
