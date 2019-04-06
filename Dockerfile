FROM tensorflow/tensorflow:1.13.1-py3

RUN apt-get update && apt-get  -y --no-install-recommends install \
    curl \
    git \
    pkg-config \
    zip \
    g++ \
    zlib1g-dev \
    unzip \
    libsnappy-dev \
    rsync

# TF 1.13 requires bazel 0.19.2
ARG BAZEL_VERSION=0.19.2
RUN mkdir /bazel && \
    curl -Lv -o /bazel/installer.sh "https://github.com/bazelbuild/bazel/releases/download/${BAZEL_VERSION}/bazel-${BAZEL_VERSION}-installer-linux-x86_64.sh" && \
    chmod +x /bazel/installer.sh && \
    /bazel/installer.sh && \
    rm -f /bazel/installer.sh
