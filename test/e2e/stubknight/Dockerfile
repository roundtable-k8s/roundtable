# Image of the e2e stub knight. The e2e suite builds the binary on the host
# (CGO_ENABLED=0 go build -o <dir>/stubknight ./test/e2e/stubknight) and runs
# docker build -f test/e2e/stubknight/Dockerfile <dir>.
FROM gcr.io/distroless/static:nonroot
COPY stubknight /stubknight
USER 65532:65532
ENTRYPOINT ["/stubknight"]
