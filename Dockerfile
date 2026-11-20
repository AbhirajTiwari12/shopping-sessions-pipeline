# Static native binary on an empty image: nothing to start but the pipeline itself.
FROM rust:1-alpine AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# The scorers are x86-64 with AVX2 and newer. TARGET_CPU=x86-64-v2 gives a build that
# also runs under emulators without AVX2; other architectures get a plain native build.
ARG TARGET_CPU=x86-64-v3
RUN if [ "$(uname -m)" = "x86_64" ]; then \
      RUSTFLAGS="-C target-cpu=${TARGET_CPU}" cargo build --release --locked --target x86_64-unknown-linux-musl \
      && cp target/x86_64-unknown-linux-musl/release/shopping-sessions /pipeline; \
    else \
      cargo build --release --locked && cp target/release/shopping-sessions /pipeline; \
    fi

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]
