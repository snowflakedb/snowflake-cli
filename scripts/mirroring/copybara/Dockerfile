# Copybara runtime image used by scripts/mirroring/lib/copybara-run.sh.
#
# Build args:
#   COPYBARA_RELEASE   Copybara release tag (e.g. v20260504). Pin this
#                      deliberately; the .sha256 from the same release is
#                      verified at build time so we fail here, not at run
#                      time, if the published asset has been tampered with.
#
# Runtime: `docker run --rm <image> <copybara CLI args>`. Caller must bind
# the repo into /workdir and /root (or similar) if it wants to provide
# git credentials.

FROM eclipse-temurin:21-jre

ARG COPYBARA_RELEASE=v20260504

# git is needed by Copybara to shell out for fetches/pushes. curl to pull
# the jar. No rust toolchain, no dev headers — this image is single-purpose.
RUN apt-get update \
 && apt-get install -y --no-install-recommends git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN set -eu; \
    curl -fsSL -o /opt/copybara_deploy.jar \
      "https://github.com/google/copybara/releases/download/${COPYBARA_RELEASE}/copybara_deploy.jar"; \
    expected=$(curl -fsSL \
      "https://github.com/google/copybara/releases/download/${COPYBARA_RELEASE}/copybara_deploy.jar.sha256" \
      | tr -d '[:space:]'); \
    echo "${expected}  /opt/copybara_deploy.jar" | sha256sum -c -

WORKDIR /workdir

ENTRYPOINT ["java", "-jar", "/opt/copybara_deploy.jar"]
