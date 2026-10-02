FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG COMMAND=native
RUN CGO_ENABLED=0 go build -trimpath -o /app ./examples/deploy/cmd/${COMMAND}

FROM scratch
COPY --from=build /app /app
USER 65532:65532
ENV BLOK_LISTEN=0.0.0.0:8080 BLOK_EXTERNAL=true
EXPOSE 8080
ENTRYPOINT ["/app"]
