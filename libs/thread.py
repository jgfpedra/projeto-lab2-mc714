"""
Fonte (estrutura de sockets - thread de escuta + conexao de saida):
GeeksforGeeks, "Socket Programming with Multi-threading in Python"
https://www.geeksforgeeks.org/python/socket-programming-multi-threading-python/
O que foi mantido: a sequencia socket() -> bind() -> listen() -> accept()
do lado que escuta, e connect() seguido de um loop de send()/recv() do
lado que conecta.

O que foi alterado: o exemplo original e um servidor de eco (inverte a
mensagem recebida) que cria uma thread nova para cada cliente conectado;
aqui cada no do anel aceita apenas uma conexao (o no anterior) e, em vez
de ecoar, acumula as mensagens recebidas em uma fila (fila_entrada) para
serem processadas pelo loop principal. O lock global do exemplo original
foi removido por nao haver multiplas conexoes simultaneas a sincronizar
neste caso.

Fonte (formato de mensagem JSON via socket):
resposta de "harry" no Stack Overflow (CC BY-SA 4.0)
https://stackoverflow.com/a/61350221
O que foi mantido: o uso de json.dumps() para serializar um dicionario
antes de enviar com sendall(), e o decode/parse do lado receptor.

O que foi alterado: o exemplo original e em Python 2 (sintaxe antiga de
print) e enviava uma unica mensagem por conexao; como aqui as mensagens
sao continuas ao longo da execucao, foi adicionado um delimitador de
quebra de linha ('\\n') para separar multiplas mensagens JSON no mesmo
fluxo TCP (funcao receber_linhas).
"""

import json
import socket
import time


def send(sock, msg_type, node_id, data, ts):
    msg = json.dumps(
        {"type": msg_type, "from": node_id, "data": data, "ts": ts})
    sock.sendall((msg + "\n").encode())


def receive_lines(sock):
    buffer = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            return
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            if line:
                yield json.loads(line.decode())


def listen_thread(listen_port, inbox):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", listen_port))
    server.listen(1)
    print(f"[Listen] Waiting for connection on port {listen_port}")
    conn, _ = server.accept()
    print("[Listen] previous node connected")
    for msg in receive_lines(conn):
        inbox.append(msg)


def connect_to_next(host, port, retries=20):
    for _ in range(retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            return sock
        except OSError:
            time.sleep(1)
    return None
