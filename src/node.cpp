/*
 * node.cpp — No do anel distribuido
 *
 * Cada container executa este binario com:
 *   ./node <NODE_ID> <TOTAL_NODES> <NEXT_HOST> <NEXT_PORT> <LISTEN_PORT>
 *
 * Topologia:
 *   node_0 -> node_1 -> node_2 -> ... -> node_(N-1) -> node_0
 *
 * Threads:
 *   - listener_thread : aceita conexao do no ANTERIOR e le mensagens
 *   - sender_thread   : conecta ao PROXIMO no e envia mensagens
 *   - main loop       : logica de negocio (CS, eleicao, eventos Lamport)
 */

#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <chrono>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "../libs/message.h"
#include "../libs/lamport.h"
#include "../libs/token_ring.h"
#include "../libs/election.h"

// -----------------------------------------------------------------------
// Fila thread-safe de mensagens recebidas
// -----------------------------------------------------------------------
struct MsgQueue {
    std::queue<Message>     q;
    std::mutex              mtx;
    std::condition_variable cv;

    void push(const Message& m) {
        std::unique_lock<std::mutex> lk(mtx);
        q.push(m);
        cv.notify_one();
    }

    bool try_pop(Message& m, int timeout_ms) {
        std::unique_lock<std::mutex> lk(mtx);
        if (!cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                         [&]{ return !q.empty(); }))
            return false;
        m = q.front(); q.pop();
        return true;
    }
};

// -----------------------------------------------------------------------
// Fila de envio (main loop -> sender thread)
// -----------------------------------------------------------------------
struct SendQueue {
    std::queue<std::string> q;
    std::mutex              mtx;
    std::condition_variable cv;

    void push(const std::string& s) {
        std::unique_lock<std::mutex> lk(mtx);
        q.push(s);
        cv.notify_one();
    }

    std::string pop() {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait(lk, [&]{ return !q.empty(); });
        std::string s = q.front(); q.pop();
        return s;
    }
};

// -----------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------
MsgQueue  inbox;
SendQueue outbox;
int       g_node_id;
int       g_total;

// -----------------------------------------------------------------------
// Utilidades de socket
// -----------------------------------------------------------------------
int make_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    bind(fd, (sockaddr*)&addr, sizeof(addr));
    listen(fd, 1);
    return fd;
}

int connect_to(const std::string& host, int port, int retries = 20) {
    for (int i = 0; i < retries; ++i) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        addrinfo hints{}, *res;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) == 0) {
            if (connect(fd, res->ai_addr, res->ai_addrlen) == 0) {
                freeaddrinfo(res);
                return fd;
            }
            freeaddrinfo(res);
        }
        close(fd);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return -1;
}

void send_all(int fd, const std::string& s) {
    size_t total = 0;
    while (total < s.size()) {
        ssize_t n = send(fd, s.c_str() + total, s.size() - total, 0);
        if (n <= 0) return;
        total += n;
    }
}

bool recv_line(int fd, std::string& out) {
    out.clear();
    char c;
    while (true) {
        ssize_t n = recv(fd, &c, 1, 0);
        if (n <= 0) return false;
        if (c == '\n') return true;
        out += c;
    }
}

// -----------------------------------------------------------------------
// Thread: escuta conexao do no anterior
// -----------------------------------------------------------------------
void listener_thread(int port) {
    int srv = make_listen_socket(port);
    std::cout << "[node " << g_node_id << "] escutando na porta " << port << std::endl;
    int cli = accept(srv, nullptr, nullptr);
    std::cout << "[node " << g_node_id << "] conexao aceita do no anterior" << std::endl;
    std::string line;
    while (recv_line(cli, line)) {
        Message m = Message::deserialize(line);
        inbox.push(m);
    }
    close(cli);
    close(srv);
}

// -----------------------------------------------------------------------
// Thread: envia mensagens para o proximo no
// -----------------------------------------------------------------------
void sender_thread(const std::string& next_host, int next_port) {
    int fd = connect_to(next_host, next_port);
    if (fd < 0) {
        std::cerr << "[node " << g_node_id << "] ERRO: nao conectou ao proximo no\n";
        return;
    }
    std::cout << "[node " << g_node_id << "] conectado a " << next_host << ":" << next_port << std::endl;
    while (true) {
        std::string s = outbox.pop();
        send_all(fd, s);
    }
    close(fd);
}

// Envia mensagem com timestamp Lamport atualizado
void send_msg(Message m, LamportClock& clk) {
    m.timestamp = clk.tick();
    m.sender_id = g_node_id;
    outbox.push(m.serialize());
}

void log(const std::string& msg, uint64_t ts) {
    std::cout << "[node " << g_node_id << " | T=" << ts << "] " << msg << std::endl;
}

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 6) {
        std::cerr << "uso: ./node <id> <total> <next_host> <next_port> <listen_port>\n";
        return 1;
    }
    g_node_id        = std::atoi(argv[1]);
    g_total          = std::atoi(argv[2]);
    std::string next_host   = argv[3];
    int         next_port   = std::atoi(argv[4]);
    int         listen_port = std::atoi(argv[5]);

    bool has_token = (g_node_id == 0);  // node 0 comeca com o token

    LamportClock clk;
    TokenRing    tr(has_token);
    Election     el(g_node_id);

    // Inicia threads de rede
    std::thread tl(listener_thread, listen_port);
    std::thread ts(sender_thread, next_host, next_port);
    tl.detach();
    ts.detach();

    // Aguarda todos os nos subirem
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Node 0 inicia a eleicao
    if (g_node_id == 0) {
        int candidate = el.start();
        Message m{MsgType::ELECTION, g_node_id, candidate, 0};
        send_msg(m, clk);
        log("iniciou eleicao com candidato " + std::to_string(candidate), clk.get());
    }

    int cs_requests = 0;

    while (true) {
        Message msg;
        bool got = inbox.try_pop(msg, 500);

        if (got) {
            uint64_t ts = clk.update(msg.timestamp);

            switch (msg.type) {

            case MsgType::ELECTION: {
                int res = el.process_election(msg.payload);
                if (res == 0) {
                    log("SOU O LIDER (ID=" + std::to_string(g_node_id) + "), enviando ELECTED", ts);
                    Message elected{MsgType::ELECTED, g_node_id, g_node_id, 0};
                    send_msg(elected, clk);
                } else if (res > 0) {
                    log("encaminhando candidato " + std::to_string(res), ts);
                    Message fwd{MsgType::ELECTION, g_node_id, res, 0};
                    send_msg(fwd, clk);
                } else {
                    log("descartou candidato " + std::to_string(msg.payload), ts);
                }
                break;
            }

            case MsgType::ELECTED: {
                bool fwd = el.process_elected(msg.payload);
                log("LIDER CONFIRMADO: node " + std::to_string(msg.payload), ts);
                if (fwd) send_msg(msg, clk);
                break;
            }

            case MsgType::TOKEN: {
                tr.receive_token();
                log("recebeu o TOKEN", ts);

                if (tr.can_enter_cs()) {
                    tr.enter_cs();
                    log("==> ENTROU na Secao Critica", clk.tick());
                    std::this_thread::sleep_for(std::chrono::milliseconds(800));
                    tr.release_cs();
                    log("<== SAIU da Secao Critica", clk.tick());
                }

                if (tr.pass_token()) {
                    log("passando TOKEN para o proximo", clk.get());
                    Message tok{MsgType::TOKEN, g_node_id, 0, 0};
                    send_msg(tok, clk);
                }
                break;
            }

            case MsgType::LAMPORT_EVENT: {
                log("recebeu evento Lamport de node " + std::to_string(msg.sender_id), ts);
                break;
            }

            default: break;
            }
        }

        // Node 0: apos eleicao, dispara o token ring
        if (el.is_done() && g_node_id == 0 && has_token && cs_requests == 0) {
            cs_requests++;
            tr.request_cs();
            log("solicitou a Secao Critica (Token Ring)", clk.tick());
            if (tr.can_enter_cs()) {
                tr.enter_cs();
                log("==> ENTROU na Secao Critica", clk.tick());
                std::this_thread::sleep_for(std::chrono::milliseconds(800));
                tr.release_cs();
                log("<== SAIU da Secao Critica", clk.tick());
            }
            if (tr.pass_token()) {
                log("passando TOKEN inicial", clk.get());
                Message tok{MsgType::TOKEN, g_node_id, 0, 0};
                send_msg(tok, clk);
            }
        }

        // Nos 1..N-1: pedem CS quando tem o token pela 1a vez
        if (el.is_done() && g_node_id != 0 && cs_requests == 0 && tr.has_token()) {
            cs_requests++;
            tr.request_cs();
            log("solicitou a Secao Critica (Token Ring)", clk.tick());
        }

        // Evento Lamport periodico a cada ~4s (nos pares, para demo)
        static auto last_event = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        if (g_node_id % 2 == 0 &&
            std::chrono::duration_cast<std::chrono::seconds>(now - last_event).count() >= 4) {
            last_event = now;
            Message ev{MsgType::LAMPORT_EVENT, g_node_id, 0, 0};
            send_msg(ev, clk);
            log("enviou evento Lamport", clk.get());
        }
    }

    return 0;
}
