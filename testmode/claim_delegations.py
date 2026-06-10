"""
Claim each AM's source delegation at the broker over Kafka, merging it into the
broker's Combined Broker Model so the orchestrator can see the resources.

On startup each AM advertises its delegation locally with no callback; the broker
only picks it up once it claims the delegation via claim_delegations(broker, did).
This runs that claim using the KafkaBroker/KafkaActor management proxies. It is
idempotent: delegations the broker already holds are skipped.

Configuration is read from environment variables (see _Config).
"""
import logging
import os
import sys
import time
from typing import List

from fabric_mb.message_bus.messages.auth_avro import AuthAvro
from fabric_mb.message_bus.messages.delegation_avro import DelegationAvro

from fabric_cf.actor.core.apis.abc_delegation import DelegationState
from fabric_cf.actor.core.common.constants import Constants
from fabric_cf.actor.core.manage.kafka.kafka_actor import KafkaActor
from fabric_cf.actor.core.manage.kafka.kafka_broker import KafkaBroker
from fabric_cf.actor.core.manage.kafka.kafka_mgmt_message_processor import KafkaMgmtMessageProcessor
from fabric_cf.actor.core.util.id import ID


class _Config:
    """Test-mode claim configuration, overridable by environment variable."""
    def __init__(self):
        self.kafka_server = os.environ.get("KAFKA_SERVER", "broker1:9092")
        self.schema_registry = os.environ.get("KAFKA_SCHEMA_REGISTRY", "http://schemaregistry:8081")
        self.security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self.key_schema = os.environ.get("KAFKA_KEY_SCHEMA", "/etc/fabric/message_bus/schema/key.avsc")
        self.value_schema = os.environ.get("KAFKA_VALUE_SCHEMA", "/etc/fabric/message_bus/schema/message.avsc")

        # Callback topic this claimer consumes management responses on.
        self.callback_topic = os.environ.get("CLAIM_CALLBACK_TOPIC", "claimer-topic")
        self.group_id = os.environ.get("KAFKA_GROUP_ID", "claimer")

        self.broker_guid = os.environ.get("BROKER_GUID", "broker-guid")
        self.broker_topic = os.environ.get("BROKER_TOPIC", "broker-topic")

        # Comma-separated list of "<guid>:<topic>" pairs, one per AM to claim from.
        ams = os.environ.get("AM_PEERS", "site1-am-guid:site1-am-topic,net1-am-guid:net1-am-topic")
        self.am_peers = [tuple(p.split(":", 1)) for p in ams.split(",") if p.strip()]

        # How long to wait for each AM's delegation to be advertised, in seconds.
        self.timeout = int(os.environ.get("CLAIM_TIMEOUT", "300"))
        self.poll_interval = int(os.environ.get("CLAIM_POLL_INTERVAL", "10"))


def _make_kafka(config: _Config, logger: logging.Logger):
    """Build the producer + management message processor for the claim client."""
    from fabric_mb.message_bus.producer import AvroProducerApi

    conf = {Constants.BOOTSTRAP_SERVERS: config.kafka_server,
            Constants.SECURITY_PROTOCOL: config.security_protocol,
            Constants.SCHEMA_REGISTRY_URL: config.schema_registry}

    producer = AvroProducerApi(producer_conf=conf, key_schema_location=config.key_schema,
                               value_schema_location=config.value_schema, logger=logger)

    consumer_conf = dict(conf)
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer_conf[Constants.GROUP_ID] = config.group_id

    message_processor = KafkaMgmtMessageProcessor(consumer_conf=consumer_conf,
                                                  key_schema_location=config.key_schema,
                                                  value_schema_location=config.value_schema,
                                                  topics=[config.callback_topic], logger=logger)
    return producer, message_processor


def _broker_held_dids(broker: KafkaBroker) -> set:
    """Delegation ids the Broker already holds (i.e. already claimed and merged)."""
    held = set()
    for delegation in (broker.get_delegations() or []):
        if delegation.get_state() != DelegationState.Failed.value:
            held.add(delegation.get_delegation_id())
    return held


def claim_all(config: _Config, logger: logging.Logger) -> int:
    producer, message_processor = _make_kafka(config, logger)
    auth = AuthAvro()
    auth.name = "claimer"
    auth.guid = "claimer-guid"

    broker = KafkaBroker(guid=ID(uid=config.broker_guid), kafka_topic=config.broker_topic, auth=auth,
                         logger=logger, message_processor=message_processor, producer=producer)
    broker.prepare(callback_topic=config.callback_topic)

    message_processor.start()
    failures = 0
    try:
        held = _broker_held_dids(broker)
        for am_guid, am_topic in config.am_peers:
            am = KafkaActor(guid=ID(uid=am_guid), kafka_topic=am_topic, auth=auth, logger=logger,
                            message_processor=message_processor, producer=producer)
            am.prepare(callback_topic=config.callback_topic)
            if not _claim_from_am(broker=broker, am=am, am_guid=am_guid, held=held, config=config, logger=logger):
                failures += 1
    finally:
        message_processor.stop()
    return failures


def _claim_from_am(*, broker: KafkaBroker, am: KafkaActor, am_guid: str, held: set, config: _Config,
                   logger: logging.Logger) -> bool:
    """Wait for an AM to advertise its delegation(s) and claim each not already held by the Broker."""
    deadline = time.time() + config.timeout
    delegations: List[DelegationAvro] = []
    while time.time() < deadline:
        delegations = am.get_delegations() or []
        if delegations:
            break
        logger.info(f"Waiting for {am_guid} to advertise a delegation ...")
        time.sleep(config.poll_interval)

    if not delegations:
        logger.error(f"AM {am_guid} advertised no delegations within {config.timeout}s; cannot claim.")
        return False

    ok = True
    for delegation in delegations:
        did = delegation.get_delegation_id()
        if did in held:
            logger.info(f"Delegation {did} from {am_guid} already held by broker; skipping.")
            continue
        logger.info(f"Claiming delegation {did} from {am_guid} at broker {config.broker_guid} ...")
        claimed = broker.claim_delegations(broker=ID(uid=am_guid), did=did)
        if claimed is None:
            logger.error(f"Claim of {did} from {am_guid} failed: {broker.get_last_error()}")
            ok = False
        else:
            logger.info(f"Claimed delegation {did} from {am_guid}.")
    return ok


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger("claimer")
    config = _Config()
    logger.info(f"Claiming delegations at broker {config.broker_guid} from AMs "
                f"{[g for g, _ in config.am_peers]} via {config.kafka_server}")
    try:
        failures = claim_all(config, logger)
    except Exception as e:
        logger.exception(f"Claim run failed: {e}")
        return 1
    if failures:
        logger.error(f"{failures} AM(s) could not be claimed.")
        return 1
    logger.info("All delegations claimed; broker CBM is populated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
