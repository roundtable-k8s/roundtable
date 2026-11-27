# Image of the e2e stub GitHub API. The Makefile target stubgithub-image builds the
# binary on the host (CGO_ENABLED=0 go build -o bin/stubgithub-image/stubgithub ./test/e2e/stubgithub)
# and runs docker build -f test/e2e/stubgithub/Dockerfile bin/stubgithub-image.
FROM gcr.io/distroless/static:nonroot
COPY stubgithub /stubgithub
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/stubgithub"]
