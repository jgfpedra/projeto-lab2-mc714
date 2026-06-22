#pragma once
#include <cstdint>
#include <algorithm>
#include <mutex>

/*
 * Relogio Logico de Lamport
 * --------------------------
 * Regra 1: antes de enviar, incrementa o clock local.
 * Regra 2: ao receber, clock = max(local, recebido) + 1.
 */
class LamportClock {
public:
    LamportClock() : clock_(0) {}

    // Evento interno ou envio de mensagem
    uint64_t tick() {
        std::lock_guard<std::mutex> lk(mtx_);
        return ++clock_;
    }

    // Recepcao de mensagem com timestamp remoto
    uint64_t update(uint64_t received) {
        std::lock_guard<std::mutex> lk(mtx_);
        clock_ = std::max(clock_, received) + 1;
        return clock_;
    }

    uint64_t get() const {
        std::lock_guard<std::mutex> lk(const_cast<std::mutex&>(mtx_));
        return clock_;
    }

private:
    uint64_t   clock_;
    std::mutex mtx_;
};
