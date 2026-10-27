FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/indexer ./cmd/indexer

# Static binary on a distroless base: no shell, non-root, ~20 MB
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/indexer /indexer
EXPOSE 8080
ENTRYPOINT ["/indexer"]
