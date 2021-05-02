"""Peer module."""
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel
from concurrent.futures import ThreadPoolExecutor

import json
import signal
import socket


class PeerType(int, Enum):
    coordinator = 1
    maker = 2
    doer = 3
    reporter = 4


class Job(BaseModel):
    next_job: Optional[str]
    id: str
    data: Dict


class JobMap(BaseModel):
    jobs: Dict[str, Job] = {}
    last_added_job: Optional[str]
    last_popped_job: Optional[str]


class PeerInfo(BaseModel):
    peer_type: Optional[PeerType]
    peer_id: str
    address: str
    jobs: JobMap
    hub: Optional[str]  # f"{hub_id}/{hub_host}:{hub_port}"
    leaves: Dict[str, str]
    version: str


class Peer:
    """A Networking peer, to implement a P2P network."""

    data: Dict[str, Any]
    info: PeerInfo
    executor: ThreadPoolExecutor

    def __init__(self, my_address: str, root_address: str) -> None:
        self.info = self.join_network(my_address, "0", root_address)
        self.data = self.info.dict()
        self.executor = ThreadPoolExecutor(max_workers=2)

    def join_network(self, my_address: str, hub_id: str, hub_address: str) -> PeerInfo:
        """Ask to join the network."""
        node = None
        nodes = [(hub_id, hub_address)]
        while nodes:
            node = nodes[0]
            nodes = nodes[1:]
            result = self.ask_to_join_hub(my_address, node[0], node[1])
            msg = result.get("msg")
            if msg == 2:
                return PeerInfo(
                    peer_id=result.get("id"),
                    address=my_address,
                    jobs=JobMap(),
                    hub=f"{hub_id}/{hub_address}",
                    leaves={},
                    version=result.get("version"),
                )
            elif msg == 3:
                nodes += [
                    (leaf_id, leaf_address)
                    for leaf_id, leaf_address in result.get("leaves", [])
                ]
            elif msg == 4:
                return PeerInfo(
                    peer_id="0",
                    address=hub_address,
                    jobs=JobMap(),
                    hub=None,
                    leaves={},
                    version=0,
                )
            else:
                raise Exception("Invalid message")
        raise Exception("Unable to join network")

    def ask_to_join_hub(self, my_address: str, hub_pid: str, hub_address: str) -> dict:
        """Ask a peer at a socket address if you can join."""
        address, port = hub_address.split(":")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((address, int(port)))
                sock.sendall(Peer.serialize({"msg": 1, "address": my_address}))

                return Peer.deserialize(sock.recv(2048))
            except ConnectionRefusedError:
                return {"msg": 4}

    def set_version(self, version: Optional[str] = None) -> None:
        """Increment version."""
        if version:
            self.info.version = version

        self.info.version = (
            f"{int(self.info.version.split(':')[0]) + 1}:{self.info.peer_id}"
        )

    def check_join(self, data: dict) -> dict:
        """Check if peer can become direct leaf of self."""
        if len(self.info.leaves) < 5:
            index = len(self.info.leaves)
            peer_id = f"{self.info.peer_id}.{index}"
            source_address = data.get("address", "")
            self.info.leaves[peer_id] = source_address
            self.set_version()
            self.broadcast(
                {
                    "msg": 8,
                    "set": {"network_size": self.data.get("network_size", 0) + 1},
                    "version": f"{self.info.version}",
                },
                source_address,
            )
            return {
                "msg": 2,
                "id": peer_id,
                "size": self.data.get("network_size", 0),
                "version": self.info.version,
            }
        else:
            return {
                "msg": 3,
                "leaves": [
                    f"{leaf_id}:{leaf_address}"
                    for leaf_id, leaf_address in self.info.leaves.items()
                ],
            }

    def broadcast(self, data: dict, address: str) -> None:
        """Broadcast data."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for leaf_address in self.info.leaves.values():
                if leaf_address != address:
                    leaf_host, leaf_port = leaf_address.split(":")
                    sock.connect((leaf_host, int(leaf_port)))
                    sock.sendall(Peer.serialize(data))
            if self.info.hub:
                _, hub_address = self.info.hub.split("/")
                if hub_address != address:
                    hub_host, hub_port = hub_address.split(":")
                    sock.connect((hub_host, int(hub_port)))
                    sock.sendall(Peer.serialize(data))

    def get_data(self, data: dict) -> dict:
        """Return data."""
        key: Optional[str] = data.get("key")
        default: Optional[str] = data.get("default")
        if key:
            return {key: self.data.get(key, default), "msg": 7}
        return {"data": self.data, "msg": 7}

    def set_data(self, data: dict) -> None:
        """Update data."""
        self.data.update(data)

    def check_version(self, version: str) -> bool:
        """Chek if a version is gr8er than current."""
        s_version, s_peer_id = version.split(":")
        s_id_parts = s_peer_id.split(".")
        c_version, c_peer_id = self.info.version.split(":")
        c_id_parts = c_peer_id.split(".")
        if int(s_version) < int(c_version):
            return False
        elif len(s_id_parts) < len(c_id_parts):
            return True
        elif len(s_id_parts) > len(c_id_parts):
            return False
        for c_id_pos, c_id_part in enumerate(c_id_parts):
            if int(c_id_part) > int(s_id_parts[c_id_pos]):
                return False
        return True

    def on_sigterm(self, signum: int, frame: Any) -> None:
        """Handle sigterm"""
        self.log.info("Received SIGTERM, raising SystemExit")
        raise SystemExit()

    def on_shutdown(self) -> None:
        """Shutdown."""
        data = {"msg": 0}
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for leaf_address in self.info.leaves.values():
                leaf_host, leaf_port = leaf_address.split(":")
                sock.connect((leaf_host, int(leaf_port)))
                sock.sendall(Peer.serialize(data))

    def main(self) -> None:
        """The main process entry point"""
        # Install signal handler
        signal.signal(signal.SIGTERM, self.on_sigterm)

        try:
            self.run()
        except SystemExit:
            self.on_shutdown()
        except Exception as ex:
            print(f"Some other error: {ex}")
            self.on_shutdown()

    def run(self) -> None:
        """Main run loop of the peer."""
        host, port = self.info.address.split(":")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, int(port)))
            sock.listen()
            while True:
                connection, addr = sock.accept()
                with connection:
                    print("Connected by", addr)
                    data = Peer.deserialize(connection.recv(2048))
                    if data.get("msg") == 0:
                        self.on_shutdown()
                    elif data.get("msg") == 1:
                        connection.sendall(Peer.serialize(self.check_join(data)))
                    elif data.get("msg") == 5:
                        self.broadcast(data, addr)
                    elif data.get("msg") == 6:
                        connection.sendall(Peer.serialize(self.get_data(data)))
                    elif data.get("msg") == 8:
                        response: Dict
                        if self.check_version(data.get("version", "0:0")):
                            self.set_data(data)
                            self.broadcast(data, addr)
                            response = {"msg": 10}
                        else:
                            response = {"msg": 9, "data": self.data}
                        connection.sendall(Peer.serialize(response))

    @classmethod
    def serialize(cls, message: dict) -> bytes:
        """Serializes a message to be sent over the wire."""
        return json.dumps(message).encode("utf-8")

    @classmethod
    def deserialize(cls, packet: bytes) -> dict:
        """Deserializes a packet sent over the wire."""
        return json.loads(packet.decode())
