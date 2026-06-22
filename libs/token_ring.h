#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>

/*
 * Exclusao Mutua por Token Ring
 * ------------------------------
 * O no com o token pode entrar na secao critica (CS).
 * Quando termina, passa o token para o proximo no do anel.
 * Se nao quer a CS, passa imediatamente.
 */
class TokenRing {
public:
    explicit TokenRing(bool has_token)
        : has_token_(has_token), wants_cs_(false), in_cs_(false) {}

    // Diz ao algoritmo que este no quer entrar na CS
    void request_cs() {
        std::lock_guard<std::mutex> lk(mtx_);
        wants_cs_ = true;
    }

    // Retorna true se o no deve entrar na CS (tem token E quer entrar)
    bool can_enter_cs() {
        std::lock_guard<std::mutex> lk(mtx_);
        return has_token_ && wants_cs_;
    }

    // Chamado quando o no efetivamente entra na CS
    void enter_cs() {
        std::lock_guard<std::mutex> lk(mtx_);
        in_cs_ = true;
    }

    // Chamado quando o no sai da CS; retorna true = deve passar o token
    bool release_cs() {
        std::lock_guard<std::mutex> lk(mtx_);
        in_cs_    = false;
        wants_cs_ = false;
        return true;  // sempre passa o token apos a CS
    }

    // Recebe o token vindo do no anterior
    void receive_token() {
        std::lock_guard<std::mutex> lk(mtx_);
        has_token_ = true;
    }

    // Passa o token adiante (perde a posse)
    bool pass_token() {
        std::lock_guard<std::mutex> lk(mtx_);
        if (in_cs_) return false;   // nao pode passar enquanto esta na CS
        has_token_ = false;
        return true;
    }

    bool has_token()  const { return has_token_.load(); }
    bool wants_cs()   const { return wants_cs_;  }
    bool in_cs()      const { return in_cs_;     }

private:
    std::atomic<bool> has_token_;
    bool              wants_cs_;
    bool              in_cs_;
    std::mutex        mtx_;
};
