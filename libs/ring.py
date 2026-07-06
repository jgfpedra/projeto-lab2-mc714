"""
    Eleicao de lider - algoritmo em anel
    Fonte: Prago2001, "LP-1/DS/Ring_Bully/ring.py"
    https://github.com/Prago2001/LP-1/blob/main/DS/Ring_Bully/ring.py

    O que foi mantido: a ideia central do algoritmo (cada processo repassa
    o maior id visto, o id que "da a volta" completa se torna coordenador,
    e a mensagem de coordenador circula avisando todo mundo).

    O que foi alterado: o original simula tudo num unico processo,
    percorrendo uma lista self.processes com prints; aqui a mensagem
    ELECTION/ELECTED e de fato enviada por socket entre processos distintos,
    e a deteccao de "voltou ao remetente original" e feita comparando o
    id do candidato com o proprio id.
"""


class RingElection:
    def __init__(self, my_id):
        self.my_id = my_id
        self.leader_id = None
        self.done = False

    def start(self):
        self.done = False
        self.leader_id = None
        return self.my_id

    def process_election(self, candidate_id):
        if candidate_id == self.my_id:
            self.leader_id = self.my_id
            self.done = True
            return None
        if candidate_id > self.my_id:
            return candidate_id
        return self.my_id

    def process_coordinator(self, leader_id):
        repass = (leader_id != self.my_id)
        self.leader_id = leader_id
        self.done = True
        return repass
