import os
import time
import json
import yaml
import random
import signal
import logging
import argparse

import utils

from utils.kafkaAvro import KafkaAVRO


# Global Variables
SIGNAL_SET = False


def signal_handler(sig, frame):
    global SIGNAL_SET
    logging.info("Signal received, checking all current sessions out")
    SIGNAL_SET = True


if __name__ == "__main__":
    # Save in memory state of the SmartShop sessions
    #  - Format: {"<session_id>": {"session_end": <timestamp>, "next_update": <timestamp>, "client_id": "<client_id>", "sku": {"<sku>": sum_qty}}}
    state_smartshop_session = dict()

    # Set signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="SmartShop Data Generator")
    parser.add_argument(
        "--config",
        dest="config",
        type=str,
        help="Config file (Default: config/localhost.yaml)",
        default=os.path.join("config", "localhost.yaml"),
    )
    parser.add_argument(
        "--verbose",
        dest="verbose",
        help="Enable verbose logs",
        action="store_true",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        help="Generate data, but do not produce to Kafka",
        action="store_true",
    )
    args = parser.parse_args()

    # Logging handler
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Read and parse config file
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    # Get SmartShop config
    MAX_SKUS = config["smartshop"]["max_skus"]
    MAX_SHOPS = config["smartshop"]["max_shops"]
    MAX_CLIENTS = config["smartshop"]["max_clients"]
    MAX_SIMULTANEOUS_SESSIONS = config["smartshop"]["max_simultaneous_sessions"]
    MAX_TRANSACIONS_PER_SECOND = config["smartshop"]["max_transacions_per_second"]
    ADD_SKU_RANGE = sorted(
        [
            config["smartshop"]["add_sku_range_min"],
            config["smartshop"]["add_sku_range_max"],
        ]
    )
    UPDATE_BASKET_RANGE = sorted(
        [
            config["smartshop"]["update_basket_range_min"],
            config["smartshop"]["update_basket_range_max"],
        ]
    )
    UPDATE_BASKET_REMOVE_SKU_PROBABILITY = config["smartshop"][
        "update_basket_remove_sku_probability"
    ]
    MAX_SKU_BASKET_SIZE = config["smartshop"]["max_basket_size"]
    CHECKOUT_TIME_RANGE = sorted(
        [
            config["smartshop"]["checkout_range_min"],
            config["smartshop"]["checkout_range_max"],
        ]
    )

    # Start Kafka producer
    if not args.dry_run:
        kafka = KafkaAVRO(config["confluent"])
        logging.info("Started Kafka producer client")

    # Start generating SmartShop events
    logging.info("Generating SmartShop events")
    while True:
        offset = time.time()

        sessions_to_remove = list()
        for session_id, value in state_smartshop_session.items():
            # Verify if any SmartShop session need to be closed (check-out)
            if value["session_end"] < time.time() or SIGNAL_SET:
                payload_status = utils.gen_payload_status(
                    session_id=session_id,
                    client_id=value["client_id"],
                    status=-1,
                    max_clients=MAX_CLIENTS,
                    max_shops=MAX_SHOPS,
                )
                sessions_to_remove.append(session_id)
                logging.debug(f"CHECK_OUT: {payload_status}")
                if not args.dry_run:
                    # Produce message to Kafka
                    kafka.produce_message(
                        kafka.checkout_topic,
                        payload_status["session_id"],
                        payload_status,
                    )

            # Update basket of active sessions
            if (
                value["session_end"] > time.time()
                and value["next_update"] < time.time()
                and not SIGNAL_SET
            ):
                state_smartshop_session[session_id][
                    "next_update"
                ] = time.time() + random.randint(*UPDATE_BASKET_RANGE)
                skus = list(value["sku"].keys())

                # Add item to the basket
                if random.random() > UPDATE_BASKET_REMOVE_SKU_PROBABILITY:
                    sku = utils.gen_sku(max_skus=MAX_SKUS)
                    qty = min(MAX_SKU_BASKET_SIZE, random.randint(*ADD_SKU_RANGE))
                    if sku in skus:
                        qty += state_smartshop_session[session_id]["sku"][sku]

                    if qty <= MAX_SKU_BASKET_SIZE:
                        state_smartshop_session[session_id]["sku"][sku] = qty
                        payload_basket = utils.gen_payload_basket(
                            session_id=session_id,
                            client_id=value["client_id"],
                            sku=sku,
                            qty=qty,
                            max_clients=MAX_CLIENTS,
                            max_skus=MAX_SKUS,
                            max_shops=MAX_SHOPS,
                        )
                        logging.debug(f"BASKET: {payload_basket}")
                        if not args.dry_run:
                            # Produce message to Kafka
                            kafka.produce_message(
                                kafka.basket_topic,
                                payload_basket["session_id"],
                                payload_basket,
                            )
                # Remove item from the basket
                else:
                    attempts = 0
                    while attempts < 100 and len(skus) > 0:
                        sku = random.choice(skus)
                        qty = state_smartshop_session[session_id]["sku"][sku]
                        if qty == 0:
                            attempts += 1
                        else:
                            qty_to_remove = min(qty, random.randint(*ADD_SKU_RANGE))
                            state_smartshop_session[session_id]["sku"][sku] = qty - min(
                                qty, qty_to_remove
                            )
                            payload_basket = utils.gen_payload_basket(
                                session_id=session_id,
                                client_id=value["client_id"],
                                sku=sku,
                                qty=-1 * qty_to_remove,
                                max_clients=MAX_CLIENTS,
                                max_skus=MAX_SKUS,
                                max_shops=MAX_SHOPS,
                            )
                            logging.debug(f"BASKET: {payload_basket}")
                            if not args.dry_run:
                                # Produce message to Kafka
                                kafka.produce_message(
                                    kafka.basket_topic,
                                    payload_basket["session_id"],
                                    payload_basket,
                                )
                            break

        # Cleanup sessions checkout
        for session in sessions_to_remove:
            state_smartshop_session.pop(session)

        # Break data generation loop
        if SIGNAL_SET:
            logging.info("Checkout completed")
            if not args.dry_run:
                logging.info(
                    "Waiting for the Kafka producer client to flush all pending messages"
                )
            break

        # Create new SmartShop session if needed
        if len(state_smartshop_session.keys()) < MAX_SIMULTANEOUS_SESSIONS:
            # Generate SmartShop session and pick a random client
            session_id = utils.gen_session_id()
            client_id = utils.gen_client_id(
                session_id,
                max_clients=MAX_CLIENTS,
            )

            # Generate checkin payload
            payload_status = utils.gen_payload_status(
                session_id=session_id,
                client_id=client_id,
                status=1,  # check-in
                max_clients=MAX_CLIENTS,
                max_shops=MAX_SHOPS,
            )
            # Update state
            state_smartshop_session[session_id] = {
                "session_end": time.time() + random.randint(*CHECKOUT_TIME_RANGE),
                "next_update": time.time() + random.randint(*UPDATE_BASKET_RANGE),
                "client_id": client_id,
                "sku": dict(),
            }
            logging.debug(f"CHECK_IN: {payload_status}")
            if not args.dry_run:
                # Produce message to Kafka
                kafka.produce_message(
                    kafka.checkin_topic,
                    payload_status["session_id"],
                    payload_status,
                )

        # Wait for the next loop
        time.sleep(max(0, (1 / MAX_TRANSACIONS_PER_SECOND) - (time.time() - offset)))

    if not args.dry_run:
        logging.info("Flushing Kafka producer")
        kafka.kafka_producer.flush()
        logging.info("Kafka producer completed, bye bye")
    else:
        logging.info("Dry-run completed, bye bye")
