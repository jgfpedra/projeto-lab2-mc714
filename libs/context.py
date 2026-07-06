from libs.thread import send


class Context:
    def __init__(self, my_id, out_sock, clock, token_ring, election):
        self.my_id = my_id
        self.out_sock = out_sock
        self.clock = clock
        self.token_ring = token_ring
        self.election = election

    def log(self, text, ts):
        print(f"[node {self.my_id} | T={ts}] {text}")

    def send(self, msg_type, data, ts):
        send(self.out_sock, msg_type, self.my_id, data, ts)
