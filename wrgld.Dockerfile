# syntax=docker/dockerfile:1.3
FROM busybox as source
RUN mkdir -p /var/lib/wrgl/data

FROM gcr.io/distroless/static:latest
LABEL org.opencontainers.image.authors="Wrangle Ltd <khoi@wrgl.co>"
LABEL org.opencontainers.image.source="https://github.com/wrgl/wrgl"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.title="Wrgld"
COPY LICENSE /usr/local/share/doc/wrgl/
COPY bin/wrgld /usr/local/bin/wrgld
COPY --from=source --chown=nonroot:nonroot /var/lib/wrgl/data /var/lib/wrgl/data
USER nonroot
EXPOSE 80
WORKDIR /var/lib/wrgl/
ENTRYPOINT ["/usr/local/bin/wrgld", "/var/lib/wrgl/data"]
