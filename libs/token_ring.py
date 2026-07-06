"""
2) Exclusao mutua por Token Ring
   Fonte: itspriyambhattacharya, "java_token_based_DME_algorithm_ring_topology"
   https://github.com/itspriyambhattacharya/java_token_based_DME_algorithm_ring_topology_MSc_AOS_Sem_2/blob/main/Process.java

   O que foi mantido: a logica de posse do token (has_token), a fila de
   pedidos (queue) e a sequencia requestToken -> checkTokenAndEnterCS ->
   enterCriticalSection -> passToken.

   O que foi alterado: o original mantem uma fila de pedidos (queue) por
   processo porque o token e enviado diretamente para quem pediu,
   percorrendo o anel ate encontrar quem esta de posse dele
   (next.requestToken(...)); como aqui o token so circula em um sentido
   fixo (cada no conhece apenas o proximo), a fila foi removida: um no
   entra na secao critica assim que recebe o token e possui um pedido
   pendente (wants_to_enter), e o repassa em seguida. Alem disso, a
   passagem do token deixou de ser uma chamada direta de metodo entre
   objetos na mesma JVM (next.setToken(...)) e passou a ser uma
   mensagem TOKEN enviada de fato pelo socket ao proximo no do anel, ja
   que os processos rodam em containers separados. Tambem foi convertido
   de Java para Python.
"""


class TokenRing:
    def __init__(self, has_token, my_id):
        self.my_id = my_id
        self.has_token = has_token
        self.wants_to_enter = False
        self.in_cs = False

    def request(self):
        self.wants_to_enter = True

    def receive_token(self):
        self.has_token = True

    def can_enter_cs(self):
        return self.has_token and self.wants_to_enter

    def enter_cs(self):
        self.in_cs = True

    def exit_cs(self):
        self.in_cs = False
        self.wants_to_enter = False

    def pass_token(self):
        if self.in_cs:
            return False
        self.has_token = False
        return True
