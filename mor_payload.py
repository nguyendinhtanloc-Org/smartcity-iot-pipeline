#!/usr/bin/env python3
import argparse
import socket
import ssl
import sys
import time

import paho.mqtt.client as mqtt


# ============================================================
# Smart City MQTT contract
# ============================================================

COMPANY_ID = "C001"

GATEWAYS = {
    "electricity": "GW_ELECTRIC_001",
    "water": "GW_WATER_001",
    "wastewater": "GW_WWTP_001",
    "lighting": "GW_LIGHT_001",
}


# ============================================================
# Helpers
# ============================================================

def force_ipv4(host: str) -> None:
    """Prefer IPv4 without breaking getaddrinfo positional calls."""
    original_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4(host_arg, port_arg, family=0, type=0, proto=0, flags=0):
        return original_getaddrinfo(
            host_arg, port_arg, socket.AF_INET, type, proto, flags
        )

    socket.getaddrinfo = getaddrinfo_ipv4


def now_local() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def build_topic_filter(company_id: str, gateway: str | None) -> str:
    """
    Real telemetry topic:
        v1/{company_id}/{gateway_id}/up/telemetry

    gateway=None => all registered gateways for that company.
    """
    if gateway:
        return f"v1/{company_id}/{gateway}/up/telemetry"
    return f"v1/{company_id}/+/up/telemetry"


# ============================================================
# MQTT callbacks
# ============================================================

received = 0
received_bytes = 0


def on_connect(client, userdata, flags, rc, properties=None):
    topic = userdata["topic"]

    if rc == 0:
        print("[MQTT] CONNECTED", flush=True)
        print(f"[MQTT] SUBSCRIBING {topic}", flush=True)

        result, mid = client.subscribe(topic, qos=userdata["qos"])
        if result != mqtt.MQTT_ERR_SUCCESS:
            print(
                f"[MQTT] SUBSCRIBE FAILED rc={result} topic={topic}",
                file=sys.stderr,
                flush=True,
            )
        else:
            print(f"[MQTT] SUBSCRIBE SENT mid={mid}", flush=True)
    else:
        print(f"[MQTT] CONNECT FAILED rc={rc}", file=sys.stderr, flush=True)


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(
        f"[MQTT] SUBSCRIBED mid={mid} qos={granted_qos}",
        flush=True,
    )


def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
    print(f"[MQTT] DISCONNECTED rc={rc}", flush=True)


def on_message(client, userdata, msg):
    global received, received_bytes

    received += 1
    received_bytes += len(msg.payload)

    try:
        payload = msg.payload.decode("utf-8")
    except UnicodeDecodeError:
        payload = msg.payload.hex()

    print(
        f"[MQTT] {msg.topic} | {payload}",
        flush=True,
    )


# ============================================================
# Main
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Smart City MQTT monitor for the real v1/{company_id}/"
            "{gateway_id}/up/telemetry contract."
        )
    )

    # --------------------------------------------------------
    # MQTT connection
    # --------------------------------------------------------

    parser.add_argument(
        "--host",
        default="dathoc.net",
        help="MQTT broker host (default: dathoc.net)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=443,
        help="MQTT broker port (default: 443 for WSS)",
    )

    parser.add_argument(
        "--ws-path",
        default="/mq",
        help="WebSocket path (default: /mq)",
    )

    parser.add_argument(
        "--username",
        default="test1",
        help="MQTT username",
    )

    parser.add_argument(
        "--password",
        default="123456",
        help="MQTT password",
    )

    parser.add_argument(
        "--qos",
        type=int,
        choices=(0, 1, 2),
        default=0,
        help="MQTT subscription QoS (default: 0)",
    )

    parser.add_argument(
        "--client-id",
        default=None,
        help="MQTT client ID; random/empty when omitted",
    )

    parser.add_argument(
        "--insecure",
        action="store_true",
        help="disable TLS certificate verification",
    )

    parser.add_argument(
        "--ca",
        default=None,
        help="custom CA certificate file",
    )

    # --------------------------------------------------------
    # Smart City topic parameters
    # --------------------------------------------------------

    parser.add_argument(
        "--company-id",
        default=COMPANY_ID,
        help=f"company ID (default: {COMPANY_ID})",
    )

    parser.add_argument(
        "--gateway",
        choices=sorted(GATEWAYS.keys()),
        default=None,
        help=(
            "gateway domain to monitor. Omit to subscribe to all registered "
            f"gateways for {COMPANY_ID}."
        ),
    )

    parser.add_argument(
        "--topic",
        default=None,
        help=(
            "explicit MQTT topic filter. When provided, it overrides "
            "--company-id/--gateway."
        ),
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # Build topic
    # --------------------------------------------------------

    gateway_id = None
    if args.gateway:
        gateway_id = GATEWAYS[args.gateway]

    topic = args.topic or build_topic_filter(args.company_id, gateway_id)

    # --------------------------------------------------------
    # Display configuration
    # --------------------------------------------------------

    print(
        f"COMPANY_ID={args.company_id}",
        flush=True,
    )
    print(
        f"GATEWAY={args.gateway or 'ALL'}",
        flush=True,
    )
    print(
        f"GATEWAY_ID={gateway_id or '+'}",
        flush=True,
    )
    print(
        f"Topic: {topic}",
        flush=True,
    )

    # --------------------------------------------------------
    # Force IPv4
    # --------------------------------------------------------

    force_ipv4(args.host)

    # --------------------------------------------------------
    # MQTT client
    # --------------------------------------------------------

    client_kwargs = {
        "userdata": {
            "topic": topic,
            "qos": args.qos,
        },
        "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
    }

    if args.client_id:
        client_kwargs["client_id"] = args.client_id

    client = mqtt.Client(
        transport="websockets",
        protocol=mqtt.MQTTv5,
        **client_kwargs,
    )

    if args.username:
        client.username_pw_set(
            args.username,
            args.password,
        )

    client.ws_set_options(path=args.ws_path)

    # TLS
    if args.ca:
        client.tls_set(
            ca_certs=args.ca,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
    else:
        client.tls_set(
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )

    if args.insecure:
        client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    print(
        f"Connecting WSS to {args.host}:{args.port}{args.ws_path}",
        flush=True,
    )

    try:
        client.connect(
            args.host,
            args.port,
            keepalive=60,
        )
    except Exception as exc:
        print(
            f"[MQTT] CONNECT ERROR: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nStopping...", flush=True)
    finally:
        try:
            client.disconnect()
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())