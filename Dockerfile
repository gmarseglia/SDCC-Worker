FROM golang:1.21.6

WORKDIR /go/src/client

COPY /worker .

RUN go build -v -o /usr/local/bin/worker ./worker.go

EXPOSE 55557

ENTRYPOINT [ "worker" ]