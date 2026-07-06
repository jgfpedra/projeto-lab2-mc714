"""
    Relogio de Lamport
    Fonte: Anol Chakraborty, "Lamport-Clock-Simulator"
    https://github.com/AnolChakraborty/Lamport-Clock-Simulator/blob/main/main.py

    O que foi mantido: a logica central da classe LamportClock
    (increment() / update() com max(local, recebido) + 1) e o uso de um
    socket TCP simples para enviar/receber o timestamp.

    O que foi alterado: removida a interface de terminal (rich/Prompt),
    removida a escolha manual de porta por input(); a classe foi
    integrada ao loop principal do no, que ja tem papel fixo
    (id, proximo no do anel) definido por argumento de linha de comando.
"""

import threading


class LamportClock:
    def __init__(self):
        self.time = 0
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.time += 1
            return self.time

    def update(self, t):
        with self.lock:
            self.time = max(t, self.time) + 1
            return self.time
