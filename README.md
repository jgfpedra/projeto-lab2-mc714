# MC714 — Lab 2: Algoritmos Distribuídos em Anel

## Algoritmos implementados

| Algoritmo | Arquivo |
|---|---|
| Relógio Lógico de Lamport | `libs/lamport.h` |
| Exclusão Mútua — Token Ring | `libs/token_ring.h` |
| Eleição de Líder — Chang-Roberts | `libs/election.h` |

## Estrutura do projeto

```
projeto/
├── Dockerfile
├── docker-compose.yml
├── README.md
├── libs/
│   ├── message.h       ← struct de mensagem + serialização
│   ├── lamport.h       ← relógio lógico de Lamport
│   ├── token_ring.h    ← exclusão mútua por token ring
│   └── election.h      ← eleição Chang-Roberts
└── src/
    └── node.cpp        ← processo principal de cada nó
```

## Como executar

### Pré-requisitos
- Docker >= 20.x
- Docker Compose >= 2.x

### Subir o cluster (4 nós em anel)

```bash
docker compose up --build
```

### Ver logs de um nó específico

```bash
docker logs node0 -f
```

### Derrubar

```bash
docker compose down
```

## Topologia

```
node0 ──► node1
  ▲           │
  │           ▼
node3 ◄── node2
```

Cada container é um processo C++ com 3 threads:
- **listener**: aceita conexão TCP do nó anterior
- **sender**: conecta-se ao próximo nó
- **main loop**: lógica dos algoritmos

## Sequência de eventos ao subir

1. Todos os nós sobem e aguardam 3 segundos (tempo para a rede estabilizar)
2. **node0** inicia a **eleição Chang-Roberts** enviando seu ID para node1
3. O algoritmo circula — o maior ID vence e se declara líder via mensagem `ELECTED`
4. Após a eleição, **node0** (que tem o token inicial) entra na **Seção Crítica**
5. Ao sair, passa o **token** para node1 → node2 → node3 → node0 ...
6. Nós pares emitem **eventos Lamport** periódicos para demonstrar o relógio lógico

## Formato das mensagens

Texto simples separado por espaços, terminado em `\n`:

```
<TYPE> <SENDER_ID> <PAYLOAD> <TIMESTAMP>
```

Tipos: `1`=LAMPORT_EVENT, `2`=TOKEN, `3`=REQUEST_CS, `4`=RELEASE_CS, `10`=ELECTION, `11`=ELECTED

## Comunicação

Sockets TCP puros (sem bibliotecas externas). Cada nó escuta na porta **9000** do seu container. O Docker Compose cria uma rede bridge `ring` onde os containers se resolvem por hostname (`node0`, `node1`, etc.).
