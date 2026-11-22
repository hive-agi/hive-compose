# syntax=docker/dockerfile:1
FROM golang:1.24.5-alpine3.22 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOOS=linux GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -trimpath -ldflags="-s -w" -o /out/orders .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/orders /orders
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/orders"]
