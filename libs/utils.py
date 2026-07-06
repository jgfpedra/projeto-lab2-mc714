import time
import datetime
import random


def handle_election(ctx, msg, ts):
    result = ctx.election.process_election(msg["data"])
    if result is None:
        ctx.log(f"I AM THE LEADER (id={ctx.my_id}), sending ELECTED", ts)
        # time.sleep(15)  # T3.3: janela para matar o lider antes do envio
        ts2 = ctx.clock.increment()
        ctx.send("ELECTED", ctx.my_id, ts2)
    else:
        ctx.log(f"forwarding candidate {result}", ts)
        ts2 = ctx.clock.increment()
        ctx.send("ELECTION", result, ts2)


def handle_elected(ctx, msg, ts):
    forward = ctx.election.process_coordinator(msg["data"])
    ctx.log(f"leader confirmed: node {msg['data']}", ts)
    if forward:
        ts2 = ctx.clock.increment()
        ctx.send("ELECTED", msg["data"], ts2)


def enter_critical_section(ctx):
    if not ctx.token_ring.can_enter_cs():
        return False
    ctx.token_ring.enter_cs()
    now = datetime.datetime.now().strftime("%H:%M:%S")
    ctx.log(f"==> ENTERED critical section {now}", ctx.clock.increment())
    # time.sleep(20) - T2.3
    time.sleep(0.8)
    ctx.token_ring.exit_cs()
    now = datetime.datetime.now().strftime("%H:%M:%S")
    ctx.log(f"<== LEFT critical section {now}", ctx.clock.increment())
    return True


def pass_token_if_possible(ctx, log_message):
    if ctx.token_ring.pass_token():
        ts2 = ctx.clock.increment()
        ctx.log(log_message, ts2)
        ctx.send("TOKEN", 0, ts2)


def handle_token(ctx, msg, ts):
    ctx.token_ring.receive_token()
    ctx.log("received the TOKEN", ts)
    enter_critical_section(ctx)
    pass_token_if_possible(ctx, "passing TOKEN to next node")


def emit_periodic_lamport_event(ctx):
    if random.random() < 0.5:
        return
    time.sleep(5)  # /T1.2
    ts = ctx.clock.increment()
    ctx.send("LAMPORT_EVENT", 0, ts)
    now = datetime.datetime.now().strftime("%H:%M:%S")
    ctx.log(f"sent Lamport event (wall-clock: {now})", ts)


def handle_lamport_event(ctx, msg, ts):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    ctx.log(f"received Lamport event from node {msg['from']} {now}", ts)


def process_inbox(ctx, inbox):
    while inbox:
        msg = inbox.pop(0)
        ts = ctx.clock.update(msg["ts"])
        handler = HANDLERS.get(msg["type"])
        if handler:
            handler(ctx, msg, ts)


def start_election_if_node_zero(ctx):
    """
    if ctx.my_id not in (0, 1):  # T3.2: dois nos iniciam eleicao
        return
    """
    if ctx.my_id != 0:
        return
    candidate = ctx.election.start()
    ts = ctx.clock.increment()
    ctx.send("ELECTION", candidate, ts)
    ctx.log(f"started election with candidate {candidate}", ts)


def request_cs_after_election(ctx, requested_cs):
    if not ctx.election.done or requested_cs:
        return requested_cs
    ctx.token_ring.request()
    ts_req = ctx.clock.increment()
    ctx.log("requested critical section (token ring)", ts_req)
    if ctx.my_id == 0 and ctx.token_ring.has_token:
        enter_critical_section(ctx)
        pass_token_if_possible(ctx, "passing initial TOKEN")
    return True


HANDLERS = {
    "ELECTION": handle_election,
    "ELECTED": handle_elected,
    "TOKEN": handle_token,
    "LAMPORT_EVENT": handle_lamport_event,
}
