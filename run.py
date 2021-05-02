import typer
import daemon
import socket
from peer import Peer


app = typer.Typer()


ROOT_HOST = "localhost"


def get_network_size(root_host: str) -> int:
    """Returns size of network at host."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((root_host, 10000))
            sock.sendall(
                Peer.serialize({"msg": 6, "key": "network_size", "default": 1})
            )
            data = Peer.deserialize(sock.recv(2048))
            print(f"data: {data}")
            return data.get("network_size", 0) or 0

        except ConnectionRefusedError:
            print("no root")
            return 0


@app.command()
def add() -> None:
    """Add a peer to the network."""
    size = get_network_size(ROOT_HOST)
    port = 10000 + size
    peer = Peer(f"{ROOT_HOST}:{port}", f"{ROOT_HOST}:10000")
    peer.main()


if __name__ == "__main__":
    app()
