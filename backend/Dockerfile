FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/livecollab .

FROM scratch
COPY --from=builder /out/livecollab /livecollab
EXPOSE 8080
ENTRYPOINT ["/livecollab"]
