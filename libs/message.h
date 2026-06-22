#pragma once
#include <cstdint>
#include <string>
#include <cstdio>

// Tipos de mensagem trafegando no anel
enum class MsgType : uint8_t {
    LAMPORT_EVENT   = 1,   // evento generico com timestamp
    TOKEN           = 2,   // token de exclusao mutua
    REQUEST_CS      = 3,   // quer entrar na secao critica
    RELEASE_CS      = 4,   // saiu da secao critica
    ELECTION        = 10,  // mensagem de eleicao Chang-Roberts
    ELECTED         = 11,  // lider eleito, propaga confirmacao
};

struct Message {
    MsgType  type;
    int      sender_id;   // no que originou
    int      payload;     // candidato (eleicao) ou flag (token)
    uint64_t timestamp;   // relogio logico de Lamport

    // Serializacao simples em texto: "TYPE SENDER PAYLOAD TIMESTAMP\n"
    std::string serialize() const {
        return std::to_string(static_cast<int>(type))  + " " +
               std::to_string(sender_id)               + " " +
               std::to_string(payload)                 + " " +
               std::to_string(timestamp)               + "\n";
    }

    static Message deserialize(const std::string& s) {
        Message m{};
        int t;
        unsigned long long ts;
        sscanf(s.c_str(), "%d %d %d %llu", &t, &m.sender_id, &m.payload, &ts);
        m.type      = static_cast<MsgType>(t);
        m.timestamp = static_cast<uint64_t>(ts);
        return m;
    }
};
