#!/usr/bin/env python3

import argparse
import json
import socket
import sys
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print(
        "Missing paho-mqtt. Install: pip3 install paho-mqtt",
        file=sys.stderr,
    )
    sys.exit(2)


class PayloadMonitor:

    def __init__(self, args):
        self.args = args
        self.count = 0
        self.started = time.monotonic()

    def on_connect(
        self,
        client,
        userdata,
        flags,
        rc,
        properties=None,
    ):
        if rc != 0:
            print(
                f"[MQTT] CONNECT FAILED rc={rc}",
                file=sys.stderr,
                flush=True,
            )
            return

        print(
            "[MQTT] CONNECTED",
            flush=True,
        )

        print(
            f"[MQTT] SUBSCRIBING {self.args.topic}",
            flush=True,
        )

        result, mid = client.subscribe(
            self.args.topic,
            qos=self.args.qos,
        )

        if result != mqtt.MQTT_ERR_SUCCESS:
            print(
                f"[MQTT] SUBSCRIBE FAILED rc={result}",
                file=sys.stderr,
                flush=True,
            )
            return

        print(
            f"[MQTT] SUBSCRIBED mid={mid}",
            flush=True,
        )

    def on_disconnect(
        self,
        client,
        userdata,
        disconnect_flags=None,
        rc=None,
        properties=None,
    ):
        print(
            f"\n[MQTT] DISCONNECTED rc={rc}",
            file=sys.stderr,
            flush=True,
        )

    def on_message(
        self,
        client,
        userdata,
        msg,
    ):
        self.count += 1

        payload = msg.payload.decode(
            "utf-8",
            errors="replace",
        )

        # In payload as fast as possible.
        print(
            f"[{self.count}] {msg.topic} {payload}",
            flush=True,
        )

    def run(self):

        # --------------------------------------------------
        # Paho MQTT over WebSocket
        # --------------------------------------------------

        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=self.args.client_id,
                transport="websockets",
            )
        except Exception:
            client = mqtt.Client(
                client_id=self.args.client_id,
                transport="websockets",
            )

        # --------------------------------------------------
        # Authentication
        # --------------------------------------------------

        client.username_pw_set(
            self.args.username,
            self.args.password,
        )

        # --------------------------------------------------
        # WebSocket path
        # --------------------------------------------------

        client.ws_set_options(
            path=self.args.ws_path,
        )

        # --------------------------------------------------
        # TLS
        # --------------------------------------------------

        if self.args.insecure:
            import ssl
            client.tls_set(cert_reqs=ssl.CERT_NONE)
        elif self.args.ca:
            client.tls_set(
                ca_certs=self.args.ca,
            )
        else:
            client.tls_set()

        # --------------------------------------------------
        # Callbacks
        # --------------------------------------------------

        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect
        client.on_message = self.on_message

        # --------------------------------------------------
        # Resolve IPv4 only
        # --------------------------------------------------

        try:

            results = socket.getaddrinfo(
                self.args.host,
                self.args.port,
                socket.AF_INET,
                socket.SOCK_STREAM,
            )

            if not results:
                raise RuntimeError(
                    f"No IPv4 address found for "
                    f"{self.args.host}"
                )

            ipv4 = results[0][4][0]

        except Exception as e:

            print(
                f"[DNS] IPv4 resolution failed: {e}",
                file=sys.stderr,
                flush=True,
            )
            return

        print(
            f"Connecting WSS to "
            f"{self.args.host}:"
            f"{self.args.port}"
            f"{self.args.ws_path}",
            flush=True,
        )

        print(
            f"IPv4: {ipv4}",
            flush=True,
        )

        print(
            f"Topic: {self.args.topic}",
            flush=True,
        )

        # --------------------------------------------------
        # Force IPv4 for Paho
        # --------------------------------------------------

        original_getaddrinfo = socket.getaddrinfo

        def ipv4_only_getaddrinfo(
            host,
            port,
            family=0,
            type=0,
            proto=0,
            flags=0,
        ):

            if host == self.args.host:

                return original_getaddrinfo(
                    host,
                    port,
                    socket.AF_INET,
                    type or socket.SOCK_STREAM,
                    proto,
                    flags,
                )

            return original_getaddrinfo(
                host,
                port,
                family,
                type,
                proto,
                flags,
            )

        socket.getaddrinfo = (
            ipv4_only_getaddrinfo
        )

        # --------------------------------------------------
        # CONNECT
        # --------------------------------------------------

        try:

            client.connect(
                self.args.host,
                self.args.port,
                keepalive=60,
            )

            print(
                "[WSS] CONNECT CALL OK",
                flush=True,
            )

            client.loop_forever()

        except KeyboardInterrupt:

            print(
                "\n\n[CTRL+C] Stopping...",
                flush=True,
            )

        except Exception as e:

            print(
                f"\n[WSS] ERROR: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
                flush=True,
            )

        finally:

            socket.getaddrinfo = (
                original_getaddrinfo
            )

            try:
                client.disconnect()
            except Exception:
                pass

            elapsed = (
                time.monotonic()
                - self.started
            )

            print(
                f"\nStopped."
                f" Messages received: "
                f"{self.count:,}"
                f"  Runtime: {elapsed:.1f}s",
                flush=True,
            )


def main():

    parser = argparse.ArgumentParser(
        description=(
            "MQTT over WSS payload monitor"
        )
    )

    parser.add_argument(
        "--host",
        default="dathoc.net",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=443,
    )

    parser.add_argument(
        "--ws-path",
        default="/mq",
    )

    parser.add_argument(
        "--username",
        required=True,
    )

    parser.add_argument(
        "--password",
        required=True,
    )

    parser.add_argument(
        "--topic",
        default="qa-smartcity/#",
    )

    parser.add_argument(
        "--qos",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--client-id",
        default=None,
    )

    parser.add_argument(
        "--ca",
        default=None,
    )

    parser.add_argument(
        "--insecure",
        action="store_true",
        help=(
            "Testing only: "
            "disable TLS certificate verification"
        ),
    )

    args = parser.parse_args()

    if not args.client_id:
        args.client_id = (
            f"wss-payload-{int(time.time())}"
        )

    PayloadMonitor(args).run()


if __name__ == "__main__":
    main()