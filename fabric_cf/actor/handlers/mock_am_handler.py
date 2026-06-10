import hashlib
import ipaddress
import time
import traceback
from typing import Tuple

from fim.slivers.network_node import NodeSliver, NodeType

from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.plugins.handlers.config_token import ConfigToken
from fabric_cf.actor.core.util.utils import sliver_to_str
from fabric_cf.actor.handlers.handler_base import HandlerBase


class MockAMHandler(HandlerBase):
    """
    Mock substrate handler for an Aggregate Manager without real hardware.

    Instead of driving substrate, it assigns a fake management IP to each compute
    sliver and returns an OK result, which moves the reservation to Active and the
    slice to StableOK. The orchestrator/AM message exchange (redeem, extend, modify,
    close, poa) is handled by the Authority actor; only the substrate is mocked.

    Configuration is read from the ``properties`` block of the resource ``handler``:

      management-ip      : fixed management IP assigned to every node sliver.
      management-ip-pool : CIDR (default 10.20.0.0/16) from which a stable,
                           per-node management IP is allocated when no fixed
                           management-ip is given.
      provisioning-delay : seconds to sleep before returning OK (default 0).
    """
    DEFAULT_POOL = "10.20.0.0/16"

    def _provisioning_delay(self) -> float:
        try:
            return float(self.properties.get("provisioning-delay", 0))
        except (TypeError, ValueError):
            return 0.0

    def _allocate_management_ip(self, *, sliver: NodeSliver) -> str:
        """
        Return the fake management IP for a node sliver. A fixed management-ip is
        used verbatim; otherwise an address is derived deterministically from the
        sliver's node id so it is unique per node and stable across create/modify.
        """
        fixed = self.properties.get("management-ip")
        if fixed:
            return str(fixed)

        pool = self.properties.get("management-ip-pool", self.DEFAULT_POOL)
        network = ipaddress.ip_network(pool, strict=False)
        usable = network.num_addresses - 2 if network.num_addresses > 2 else network.num_addresses
        key = str(getattr(sliver, "node_id", None) or sliver.get_name())
        offset = (int(hashlib.sha256(key.encode()).hexdigest(), 16) % usable) + 1
        return str(network.network_address + offset)

    def _provision_node(self, *, sliver: NodeSliver):
        """Assign a fake management IP (and instance id) to a compute sliver."""
        management_ip = self._allocate_management_ip(sliver=sliver)
        sliver.management_ip = management_ip
        if sliver.label_allocations is not None and sliver.label_allocations.instance is None:
            sliver.label_allocations.instance = f"instance-{sliver.get_name()}"
        self.get_logger().info(f"MockAM assigned management_ip {management_ip} to node {sliver.get_name()}")

    def create(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"MockAM create invoked for unit: {unit}")
            sliver = unit.get_sliver()
            self.get_logger().info(f"MockAM creating sliver: {sliver_to_str(sliver=sliver)}")

            delay = self._provisioning_delay()
            if delay:
                time.sleep(delay)

            if isinstance(sliver, NodeSliver) and sliver.get_type() == NodeType.VM:
                self._provision_node(sliver=sliver)

            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_CREATE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        except Exception as e:
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_CREATE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
        finally:
            self.get_logger().info("MockAM create completed")
        return result, unit

    def modify(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"MockAM modify invoked for unit: {unit}")
            sliver = unit.get_modified()
            if isinstance(sliver, NodeSliver) and sliver.get_type() == NodeType.VM:
                self._provision_node(sliver=sliver)

            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_MODIFY,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        except Exception as e:
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_MODIFY,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
        finally:
            self.get_logger().info("MockAM modify completed")
        return result, unit

    def delete(self, unit: ConfigToken) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"MockAM delete invoked for unit: {unit}")
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_DELETE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
        except Exception as e:
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_DELETE,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0}
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
        finally:
            self.get_logger().info("MockAM delete completed")
        return result, unit

    def poa(self, unit: ConfigToken, data: dict) -> Tuple[dict, ConfigToken]:
        result = None
        try:
            self.get_logger().info(f"MockAM poa invoked for unit: {unit}")
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_POA,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_OK,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0,
                      Constants.PROPERTY_POA_INFO: {"operation": data.get("operation"),
                                                    "poa_id": data.get("poa_id"),
                                                    "code": Constants.RESULT_CODE_OK}}
        except Exception as e:
            result = {Constants.PROPERTY_TARGET_NAME: Constants.TARGET_POA,
                      Constants.PROPERTY_TARGET_RESULT_CODE: Constants.RESULT_CODE_EXCEPTION,
                      Constants.PROPERTY_ACTION_SEQUENCE_NUMBER: 0,
                      Constants.PROPERTY_POA_INFO: {"operation": data.get("operation"),
                                                    "poa_id": data.get("poa_id"),
                                                    "code": Constants.RESULT_CODE_EXCEPTION}}
            self.get_logger().error(e)
            self.get_logger().error(traceback.format_exc())
        finally:
            self.get_logger().info("MockAM poa completed")
        return result, unit

    def clean_restart(self):
        self.get_logger().info("MockAM clean_restart invoked (no-op)")
