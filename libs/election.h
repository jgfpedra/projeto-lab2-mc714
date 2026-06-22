#pragma once
#include <atomic>
#include <mutex>

/*
 * Eleicao de Lider — Chang-Roberts (para topologia em anel)
 * -----------------------------------------------------------
 * Cada no envia seu ID para o proximo.
 * Ao receber um ID:
 *   - se ID recebido > ID proprio: encaminha (candidato melhor)
 *   - se ID recebido < ID proprio: descarta (candidato pior)
 *   - se ID recebido == ID proprio: EU SOU O LIDER, envia ELECTED
 * Ao receber ELECTED: registra o lider e encaminha (ate voltar ao lider)
 */
class Election {
public:
    explicit Election(int my_id)
        : my_id_(my_id), leader_id_(-1), running_(false), done_(false) {}

    // Inicia eleicao: retorna o ID a ser enviado ao proximo no
    int start() {
        std::lock_guard<std::mutex> lk(mtx_);
        running_ = true;
        done_    = false;
        leader_id_ = -1;
        return my_id_;
    }

    // Processa mensagem ELECTION com o candidato recebido.
    // Retorna:
    //   > 0  => encaminhar este ID como ELECTION
    //   == 0 => sou o lider, enviar ELECTED com my_id_
    //   < 0  => descartar (ID menor que o proprio, nao encaminha)
    int process_election(int candidate_id) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (candidate_id > my_id_) {
            return candidate_id;   // encaminha
        } else if (candidate_id == my_id_) {
            leader_id_ = my_id_;
            done_      = true;
            running_   = false;
            return 0;             // sou o lider
        }
        return -1;                // descarta
    }

    // Processa mensagem ELECTED com o lider confirmado.
    // Retorna false quando a mensagem voltou ao lider (parar propagacao).
    bool process_elected(int leader_id) {
        std::lock_guard<std::mutex> lk(mtx_);
        leader_id_ = leader_id;
        done_      = true;
        running_   = false;
        // Se eu sou o lider e a msg voltou, para de propagar
        return (leader_id != my_id_);
    }

    int  leader_id() const { return leader_id_.load(); }
    bool is_done()   const { return done_.load();      }
    bool running()   const { return running_.load();   }
    int  my_id()     const { return my_id_;            }

private:
    int               my_id_;
    std::atomic<int>  leader_id_;
    std::atomic<bool> running_;
    std::atomic<bool> done_;
    std::mutex        mtx_;
};
