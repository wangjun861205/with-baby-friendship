FROM ubuntu:20.04
WORKDIR /app
COPY target/release/with-baby-auth .
ENTRYPOINT ["/app/with-baby-friendship"]