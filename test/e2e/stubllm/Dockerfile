# Image of the e2e stub LLM. The Makefile target stubllm-image builds the binary
# on the host (CGO_ENABLED=0 go build -o bin/stubllm-image/stubllm ./test/e2e/stubllm)
# and runs docker build -f test/e2e/stubllm/Dockerfile bin/stubllm-image.
FROM gcr.io/distroless/static:nonroot
COPY stubllm /stubllm
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/stubllm"]
