FROM golang:1.26

RUN apt-get update && \
    apt-get install -y --no-install-recommends iptables iproute2 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY go.mod go.sum* .
RUN go mod download

COPY . .

RUN go build -o server ./cmd/key-value-go

VOLUME ["/app/data"]

EXPOSE 8080
ENTRYPOINT ["./server"]
