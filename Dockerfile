FROM archlinux:latest

RUN pacman -Syu --noconfirm && \
    pacman -S --noconfirm gcc make && \
    pacman -Scc --noconfirm

WORKDIR /app

COPY libs/ ./libs/
COPY src/  ./src/

RUN g++ -std=c++17 -O2 -Wall -pthread \
    src/node.cpp \
    -o node

ENTRYPOINT ["./node"]
