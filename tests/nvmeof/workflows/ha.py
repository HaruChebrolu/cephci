"""
NVMe High Availability Module.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor

from ceph.ceph import CommandFailed
from ceph.ceph_admin.daemon import Daemon
from ceph.ceph_admin.host import Host
from ceph.ceph_admin.orch import Orch
from ceph.parallel import parallel
from ceph.utils import get_node_by_id, get_openstack_driver
from ceph.waiter import WaitUntil
from tests.io.io_utils import get_max_clat_from_fio_output
from tests.nvmeof.workflows.initiator import NVMeInitiator, validate_initiator
from tests.nvmeof.workflows.nvme_gateway import NVMeGateway
from tests.nvmeof.workflows.utils import (
    ana_states,
    catogorize,
    check_gateway_availability,
    compare_client_namespace,
    fetch_namespaces,
    get_optimized_state,
)
from utility.log import Log
from utility.retry import retry
from utility.utils import log_json_dump

LOG = Log(__name__)


class HighAvailability:
    def __init__(self, ceph_cluster, gateways, **config):
        """Initialize NVMeoF Gateway High Availability class.

        Args:
            cluster: Ceph cluster
            gateways: Gateway node Ids
            config: HA config
        """
        self.cluster = ceph_cluster
        self.config = config
        self.gateways = []
        self.mtls = config.get("mtls")
        self.gateway_group = config.get("gw_group", "")
        self.orch = Orch(cluster=self.cluster, **{})
        self.daemon = Daemon(cluster=self.cluster, **{})
        self.host = Host(cluster=self.cluster, **{})
        self.nvme_pool = config["rbd_pool"]
        self.clients = []
        self.initiators = {}

        for gateway in gateways:
            gw_node = get_node_by_id(self.cluster, gateway)
            self.gateways.append(NVMeGateway(gw_node, self.mtls))

        self.ana_ids = [i.ana_group_id for i in self.gateways]
        self.fail_ops = {
            "systemctl": self.system_control,
            "daemon": self.ceph_daemon,
            "power_on_off": self.power_on_off,
            "maintanence_mode": self.maintanence_mode,
        }

    def get_or_create_initiator(self, node_id, nqn):
        """Get existing NVMeInitiator or create a new one for each (node_id, nqn)."""
        key = (node_id, nqn)  # Use both as dictionary key

        if key not in self.initiators:
            node = get_node_by_id(self.cluster, node_id)
            self.initiators[key] = NVMeInitiator(node, self.gateways[0], nqn)

        return self.initiators[key]

    def system_control(self, gateway, action, wait_for_active_state=True):
        """SystemCtl methods to control nvme unit service states.

        Args:
            gateway: NVMe gateway object.
            action: systemctl "stop"|"start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the service unit becomes active.
            False: wait for the service unit becomes inactive.
            None: do not wait, return

        Returns:
            Boolean
        """
        ops = {
            "start": gateway.systemctl.start,
            "stop": gateway.systemctl.stop,
            "is-active": gateway.systemctl.is_active,
        }

        unit_service = gateway.system_unit_id
        op = ops[action]
        op(unit_service)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(unit_service)
            if _active == wait_for_active_state:
                LOG.info(
                    f"[ {unit_service} ] {action}ing NVMeofGW service is successfull."
                )
                return True
            LOG.warning(
                f"[ {unit_service} ] {action}ing NVMeofGW service is still not successfull. check again"
            )

        if w.expired:
            LOG.error(
                f"[ {unit_service} ] {action}ing NVMeofGW service failed even after 300s timeout.."
            )

        return False

    def nvmeof_daemon_state(self, name):
        """Return True if nvmeof daemon is running else False."""
        daemon_type, daemon_id = name.split(".", 1)
        ps_args = {
            "base_cmd_args": {"format": "json"},
            "args": {
                "daemon_type": daemon_type,
                "daemon_id": daemon_id,
                "refresh": True,
            },
        }
        out, _ = self.orch.ps(ps_args)
        out = json.loads(out)
        if out[0]["status"] == 1:
            return True
        elif out[0]["status"] in [-1, 0]:
            return False

    def ceph_daemon(self, gateway, action, wait_for_active_state=True):
        """Ceph daemon commands to control nvme daemon states.

        Args:
            gateway: NVMe gateway object.
            action: daemon orch "stop"|"start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the daemon becomes active.
            False: wait for the daemon becomes inactive.
            None: do not wait, return

        Returns:
            Boolean
        """
        ops = {
            "start": self.daemon.start,
            "stop": self.daemon.stop,
            "is-active": self.nvmeof_daemon_state,
        }
        daemon = gateway.daemon_name

        action_args = {"command": action, "pos_args": [daemon]}

        op = ops[action]
        op(action_args)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(daemon)
            if _active == wait_for_active_state:
                LOG.info(f"[ {daemon} ] {action}ing NVMeofGW Daemon is successfull.")
                return True
            LOG.warning(
                f"[ {daemon} ] {action}ing NVMeofGW Daemon is still not successfull. check again"
            )

        if w.expired:
            LOG.error(
                f"[ {daemon} ] {action}ing NVMeofGW Daemon failed even after 300s timeout.."
            )

        return False

    def daemon_redeploy(self, gateway):
        """Ceph daemon redeploy commands to control nvme daemon states.

        Args:
            gateway: NVMe gateway object.

        - wait_for_active_state:
            True: wait for the daemon becomes active.
            False: wait for the daemon becomes inactive.
            None: do not wait, return

        Returns:
            Boolean
        """
        daemon = gateway.daemon_name

        action_args = {"command": "redeploy", "pos_args": [daemon]}
        out = self.daemon.redeploy(action_args)
        if "Scheduled" not in out[0]:
            raise Exception(f"[ {daemon} ]: Error in redeploying NVMe Service ")

        # Wait for the daemon to become active
        for w in WaitUntil(timeout=300):
            _active = self.nvmeof_daemon_state(daemon)
            if _active:
                LOG.info(f"[ {daemon} ] redeploying NVMeofGW Daemon is successfull.")
                break
            else:
                LOG.warning(
                    f"[ {daemon} ] redeploying NVMeofGW Daemon is still not successfull. check again"
                )

        # If the loop times out, log an error and return False
        if w.expired:
            LOG.error(
                f"[ {daemon} ] redeploying NVMeofGW Daemon failed even after 300s timeout.."
            )
            return False

        # Only check the ANA states if the daemon is active
        states = ana_states()

        # Validate the service state for each host
        for host, state in states.items():
            daemon_host = f"client.{daemon}"
            if host == daemon_host:
                if state["Availability"] == "AVAILABLE":
                    LOG.info(f"[ {daemon} ] NVMeofGW service is AVAILABLE.")
                    return True
                else:
                    raise Exception(f"[ {daemon} ] NVMeofGW service is UNAVAILABLE.")

    def is_node_active(self, driver, node):
        """Return true if the given node is powered on and false if powered off"""
        op = driver.ex_get_node_details(node)
        if op.state == "running":
            return True
        elif op.state == "stopped":
            return False

    def maintanence_mode(self, gateway, action, wait_for_active_state=None):
        """Ceph daemon commands to control nvme daemon states.

        Args:
            gateway: NVMe gateway object.
            action: "stop" | "start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the node to power on.
            False: wait for the node to power off.
            None: do not wait, return

        Returns:
            Boolean
        """
        ops = {
            "start": self.host.exit,
            "stop": self.host.enter,
            "is-active": self.nvmeof_daemon_state,
        }
        daemon = gateway.daemon_name
        gateway_node = daemon.split(".")[-2]

        action_args = {"command": action, "args": {"node": gateway_node}}

        op = ops[action]
        op(action_args)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(daemon)
            if _active == wait_for_active_state:
                LOG.info(f"[ {daemon} ] {action}ing NVMeofGW Daemon is successfull.")
                return True
            LOG.warning(
                f"[ {daemon} ] {action}ing NVMeofGW Daemon is still not successfull. check again"
            )

        if w.expired:
            LOG.error(
                f"[ {daemon} ] {action}ing NVMeofGW Daemon failed even after 300s timeout.."
            )

        return False

    def power_on_off(self, gateway, action, wait_for_active_state=None):
        """Power on and off nvme daemon nodes.

        Args:
            gateway: NVMe gateway object.
            action: "stop" | "start"
            wait_for_active_state: wait for the active using bool value.

        - wait_for_active_state:
            True: wait for the node to power on.
            False: wait for the node to power off.
            None: do not wait, return

        Returns:
            Boolean
        """
        osp_cred = self.config.get("osp_cred")
        driver = get_openstack_driver(osp_cred)
        ops = {
            "start": driver.ex_start_node,
            "stop": driver.ex_stop_node,
            "is-active": self.is_node_active,
        }
        nodename = gateway.node.hostname.lower().replace("-", "_")
        driver_node = next(
            (
                node
                for node in driver.list_nodes()
                if node.name.lower().replace("-", "_") == nodename
            ),
            None,
        )

        op = ops[action]
        op(driver_node)

        if wait_for_active_state is None:
            return

        is_active = ops["is-active"]
        for w in WaitUntil(timeout=300):
            _active = is_active(driver, driver_node)
            if _active == wait_for_active_state:
                LOG.info(f"[ {nodename} ] {action} is successfull.")
                return True
            LOG.warning(
                f"[ {nodename} ] {action} is still not successfull. check again"
            )

        if w.expired:
            LOG.error(f"[ {nodename} ] {action} failed even after 300s timeout..")

        return False

    def failover(self, gateway, fail_tool, namespaces):
        """HA Failover on the NVMeoF Gateways.

        Initiate Failover
        - List the gateways which not have failures.
        - Initiate failures on GWs which has to be down systemctl/daemon stop
        - Validate the ANA states of failed GWs are optimized in one of the other working GWs.

        Post Failover Validation
        - List out namespaces associated with the failed Gateways using ANA group ids.
        - Check for 5 Consecutive times for the increments in write/read to validate IO continuation.
        """
        hostname = gateway.hostname
        io_tasks = []
        executor = ThreadPoolExecutor(max_workers=len(self.clients))

        try:
            # Start IO Execution
            for initiator in self.clients:
                io_tasks.append(executor.submit(initiator.start_fio, "1G"))
            time.sleep(20)  # time sleep for IO to Kick-in

            self.validate_io(namespaces)

            # Initiate Failover
            fail_op = self.fail_ops[fail_tool]
            LOG.info(
                f"[ {hostname} ]: Failing Over NVMe Service using {fail_tool} command"
            )
            res = fail_op(gateway=gateway, action="stop", wait_for_active_state=False)
            if not res:
                raise Exception(
                    f"[ {hostname} ]: Error in stopping NVMe Service using {fail_tool} command "
                )

            # Wait until 60 seconds
            for w in WaitUntil():
                # Check for gateway unavailability
                if check_gateway_availability(
                    gateway.ana_group_id, state="UNAVAILABLE"
                ):
                    LOG.info(f"[ {hostname} ] NVMeofGW service is UNAVAILABLE.")
                    active = get_optimized_state(gateway.ana_group_id)

                    # Find optimized path
                    # Condition to fail if multiple Active path exists for a gateway.
                    if active and 1 <= len(active) < 2:
                        end_counter, end_time = get_current_timestamp()
                        LOG.info(
                            f"{list(active[0])} is new and only Active GW for failed {hostname}"
                        )
                        break

                    if len(active) > 1:
                        raise Exception(
                            f"[ {hostname} ] Found more than one Active path - {log_json_dump(active)}"
                        )
                LOG.warning(f"[ {hostname} ] is still in AVAILABLE state..")

            if w.expired:
                raise TimeoutError(
                    f"[ {hostname} ] Failover of NVMeofGW service failed after 60s timeout.."
                )
            self.validate_io(namespaces)

            return {
                "failed-gw": gateway,
            }

        except BaseException as err:  # noqa
            raise Exception(err)

        finally:
            # Wait for IO to complete and collect FIO outputs
            if io_tasks:
                LOG.info("Waiting for completion of IOs.")
                executor.shutdown(wait=True, cancel_futures=True)
                fio_outputs = []

                for task in io_tasks:
                    try:
                        fio_outputs.append(task.result())
                    except Exception as e:
                        LOG.error(f"FIO execution failed: {e}")

            # Extract failover time
            for idx, output in enumerate(fio_outputs):
                try:
                    max_clat_in_ms = get_max_clat_from_fio_output(output[0][0])
                    max_clat_in_sec = max_clat_in_ms / 1000
                    LOG.info(f"Failover time for {max_clat_in_sec} ms")
                except Exception as e:
                    LOG.error(f"Failed to parse FIO output: {e}")

    def failback(self, gateway, fail_tool, namespaces):
        """Failback the Gateways.

        Args:
            gateway: Gateway to be fail-back.
            fail_tool: tool to fail the GW service
        """
        hostname = gateway.hostname
        io_tasks = []
        executor = ThreadPoolExecutor(max_workers=len(self.clients))

        # Initiate Fail-back
        fail_op = self.fail_ops[fail_tool]
        LOG.info(
            f"[ {hostname} ]: Failback / Restore Gateway using {fail_tool} command"
        )
        try:
            # Start IO Execution
            for initiator in self.clients:
                io_tasks.append(executor.submit(initiator.start_fio, "2G"))
            time.sleep(20)  # time sleep for IO to Kick-in

            self.validate_io(namespaces)

            res = fail_op(gateway=gateway, action="start", wait_for_active_state=True)
            if not res:
                raise Exception(
                    f"[ {hostname} ]: Error in starting NVMe Service using {fail_tool} command "
                )

            for w in WaitUntil():
                # Check for gateway availability
                if check_gateway_availability(gateway.ana_group_id):
                    LOG.info(f"[ {hostname} ] NVMeofGW service is AVAILABLE.")

                    active = get_optimized_state(gateway.ana_group_id)
                    if active and 1 <= len(active) < 2:
                        state = active[0]

                        # check gateway for its own original path.
                        if gateway.ana_group["name"] in state:
                            end_counter, end_time = get_current_timestamp()
                            LOG.info(
                                f"{hostname} restored to original path - {log_json_dump(state)}"
                            )
                            break

                    if len(active) > 1:
                        raise Exception(
                            f"[ {hostname} ] More than one Active path found - {log_json_dump(active)}"
                        )
                    LOG.warning(f"[ {hostname} ] No Active path found")
                    continue

                LOG.warning(f"[ {hostname} ] is still not in AVAILABLE state..")
                continue

            if w.expired:
                raise TimeoutError(
                    f"[ {hostname} ] Fail-back of NVMeofGW service failed even after 60s timeout.."
                )
            self.validate_io(namespaces)

            return {
                "failed-gw": gateway,
            }

        except BaseException as err:  # noqa
            raise Exception(err)

        finally:
            # Wait for IO to complete and collect FIO outputs
            if io_tasks:
                LOG.info("Waiting for completion of IOs.")
                executor.shutdown(wait=True, cancel_futures=True)
                fio_outputs = []

                for task in io_tasks:
                    try:
                        fio_outputs.append(task.result())
                    except Exception as e:
                        LOG.error(f"FIO execution failed: {e}")

            # Extract failback time
            for idx, output in enumerate(fio_outputs):
                try:
                    max_clat_in_ms = get_max_clat_from_fio_output(output[0][0])
                    max_clat_in_sec = max_clat_in_ms / 1000
                    LOG.info(f"Failback time for {max_clat_in_sec} ms")
                except Exception as e:
                    LOG.error(f"Failed to parse FIO output: {e}")

    def prepare_io_execution(self, io_clients):
        """Prepare FIO Execution.

        initiators:                             # Configure Initiators with all pre-req
          - nqn: connect-all
            listener_port: 4420
            node: node10
        """
        for io_client in io_clients:
            nqn = io_client.get("nqn")
            if io_client.get("subnqn"):
                nqn = io_client.get("subnqn")
            client = self.get_or_create_initiator(io_client["node"], nqn)
            client.connect_targets(io_client)
            if client not in self.clients:
                self.clients.append(client)

    @retry((IOError, TimeoutError, CommandFailed), tries=7, delay=2)
    def validate_io(self, namespaces):
        """Validate Continuous IO on namespaces.

        - Collect rbd disk usage info for each rbd image.
        - Validate written bytes value is incremental.

        Args:
            namespaces: list of namespaces
        """

        def io_value(ns):
            sub_ns, pool, image = ns.rsplit("|", 2)
            count = 3
            samples = []
            for _ in range(count):
                out, _ = self.orch.shell(
                    args=[f"rbd --format json du {pool}/{image}"], timeout=600
                )
                out = json.loads(out)["images"][0]
                samples.append(out)
                time.sleep(6)
            return sub_ns, f"{pool}/{image}", samples

        def validate_incremetal_io(write_samples):
            for i in range(len(write_samples) - 1):
                if write_samples[i] >= write_samples[i + 1]:
                    return False
            return True

        with parallel() as p:
            for namespace in namespaces:
                p.spawn(io_value, namespace)

            for result in p:
                subsys, pool_img, samples = result
                res = [i["used_size"] for i in samples]

                LOG.info(
                    f"[ {subsys}|{pool_img} ] RBD DU Detailed - {log_json_dump(samples)}"
                )
                LOG.info(f"[ {subsys}|{pool_img} ] RBD DU samples - {res}")
                if not validate_incremetal_io(res):
                    raise IOError(
                        f"[ {subsys}|{pool_img} ] IO is not progressing - {res}"
                    )
                LOG.info(f"IO validation for {subsys}|{pool_img} is successful.")

        LOG.info("IO Validation is Successfull on all RBD images..")

    def run(self):
        """Execute the HA failover and failback with IO validation."""
        fail_methods = self.config["fault-injection-methods"]
        initiators = self.config["initiators"]

        try:
            # Prepare FIO Execution
            namespaces = fetch_namespaces(self.gateways[0])
            self.prepare_io_execution(initiators)

            # Check for targets at clients
            compare_client_namespace([i["uuid"] for i in namespaces])

            repeat_ha_count = self.config.get("repeat_ha_count", 1)

            # Failover and Failback
            for i in range(0, repeat_ha_count):
                LOG.info(f"Failover and failback execution for iteration number {i}")
                for fail_method in fail_methods:
                    fail_tool = fail_method["tool"]
                    nodes = fail_method["nodes"]
                    if not isinstance(nodes, list):
                        nodes = [nodes]

                    LOG.info(
                        f"Failover and Failback execution on {nodes} using {fail_tool}"
                    )
                    log_json_dump(fail_method)

                    fail_gws, _ = catogorize(nodes)
                    fail_gw_ana_ids = []
                    namespaces = []
                    all_failed_ns = {}
                    for gw in fail_gws:
                        fail_gw_ana_ids.append(gw.ana_group_id)
                        namespaces_gw = fetch_namespaces(
                            gw, [gw.ana_group_id], get_list=True
                        )
                        ns_list = [ns.get("list") for ns in namespaces_gw]
                        ns_info = [ns.get("info") for ns in namespaces_gw]
                        LOG.info(
                            f"Namespaces for failed gateway {gw.node.ip_address} before failover are {ns_list}"
                        )
                        namespaces.extend(ns_info)
                        all_failed_ns.update({gw.ana_group_id: ns_list})
                        validate_initiator(self.clients, gw, ns_list)

                    # Fail Over
                    with parallel() as p:
                        for gw in fail_gws:
                            if fail_tool == "daemon_redeploy":
                                p.spawn(self.daemon_redeploy, gw)
                            else:
                                p.spawn(self.failover, gw, fail_tool, namespaces)
                        for result in p:
                            if not isinstance(result, dict):
                                raise Exception("Failover failed")
                            failed_gw = result.pop("failed-gw", None)
                            if not failed_gw:
                                raise Exception("Failover failed")

                        for gw in fail_gws:
                            active = get_optimized_state(gw.ana_group_id)
                            active_gw = list(active[0])[0]
                            LOG.info(log_json_dump(result))
                            active_gw_obj = [
                                gw
                                for gw in self.gateways
                                if gw.daemon_name in active_gw
                            ][0]
                            LOG.info(
                                f"Active gateway after failover for {gw.node.ip_address} is \
                                {active_gw_obj.node.ip_address}"
                            )
                            namespaces_gw = fetch_namespaces(
                                active_gw_obj, [gw.ana_group_id], get_list=True
                            )
                            ns_list = [ns.get("list") for ns in namespaces_gw]
                            LOG.info(
                                f"Namespaces for failed gateway {gw.node.ip_address} after \
                                    failover are {ns_list}"
                            )
                            validate_initiator(self.clients, active_gw_obj, ns_list, gw)

                    # Fail Back
                    if fail_tool != "daemon_redeploy":
                        with parallel() as p:
                            for gw in fail_gws:
                                p.spawn(self.failback, gw, fail_tool, namespaces)
                            for result in p:
                                if not isinstance(result, dict):
                                    raise Exception("Failback failed")
                                failed_gw = result.pop("failed-gw", None)
                                LOG.info(log_json_dump(result))
                                if not failed_gw:
                                    raise Exception("Failback failed")
                                namespaces_gw = fetch_namespaces(
                                    failed_gw, [failed_gw.ana_group_id], get_list=True
                                )
                                ns_list = [ns.get("list") for ns in namespaces_gw]
                                LOG.info(
                                    f"Namespaces for failed gateway {failed_gw.node.ip_address} after \
                                        failback are {ns_list}"
                                )
                                LOG.info(
                                    f"Active gateway after failback is {failed_gw.node.ip_address}"
                                )
                                validate_initiator(self.clients, failed_gw, ns_list)
                        time.sleep(20)
        except BaseException as err:  # noqa
            raise Exception(err)
