import sys
import threading
import time


from libs.ring import RingElection
from libs.lamport import LamportClock
from libs.token_ring import TokenRing
from libs.thread import listen_thread, connect_to_next
from libs.context import Context
from libs.utils import (
    start_election_if_node_zero,
    process_inbox,
    request_cs_after_election,
    emit_periodic_lamport_event
)


def main():
    if len(sys.argv) < 5:
        print("usage: node.py <id> <next_host> " +
              "<next_port> <listen_port>")
        sys.exit(1)

    my_id = int(sys.argv[1])
    next_host = sys.argv[2]
    next_port = int(sys.argv[3])
    listen_port = int(sys.argv[4])

    clock = LamportClock()
    token_ring = TokenRing(has_token=(my_id == 0), my_id=my_id)
    election = RingElection(my_id)

    inbox = []
    threading.Thread(target=listen_thread, args=(
        listen_port, inbox), daemon=True).start()

    time.sleep(2)
    out_sock = connect_to_next(next_host, next_port)
    if out_sock is None:
        print(f"[node {my_id}] ERROR: could not connect to next node")
        sys.exit(1)
    print(f"[node {my_id}] connected to {next_host}:{next_port}")

    ctx = Context(my_id, out_sock, clock, token_ring, election)
    start_election_if_node_zero(ctx)

    requested_cs = False
    while True:
        process_inbox(ctx, inbox)
        requested_cs = request_cs_after_election(ctx, requested_cs)
        emit_periodic_lamport_event(ctx)
        time.sleep(4)


if __name__ == "__main__":
    main()
