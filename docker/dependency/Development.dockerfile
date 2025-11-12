# syntax=docker/dockerfile:1
# The development image adds common development tools we use during development and the CI uses for the pre-build-check
ARG TAG=latest
FROM nebulastream/nes-development-dependency:${TAG}

ARG ANTLR4_VERSION=4.13.2

RUN apt-get update -y && apt-get install -y \
        clang-format-${LLVM_TOOLCHAIN_VERSION} \
        clang-tidy-${LLVM_TOOLCHAIN_VERSION} \
        lldb-${LLVM_TOOLCHAIN_VERSION} \
        gdb \
        python3-venv \
        python3-bs4 \
        openjdk-21-jre-headless \
        libgeos-dev \
        libproj-dev \
        libgsl-dev \
        libjson-c-dev \
        autoconf \
        automake \
        libtool \
        pkg-config

# The vcpkg port of antlr requires the jar to be available somewhere
ADD --checksum=sha256:eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76 --chmod=744 \
  https://www.antlr.org/download/antlr-${ANTLR4_VERSION}-complete.jar /opt/antlr-${ANTLR4_VERSION}-complete.jar

RUN git clone https://github.com/aras-p/ClangBuildAnalyzer.git \
    && cmake -B ClangBuildAnalyzer/build -S ClangBuildAnalyzer -DCMAKE_INSTALL_PREFIX=/usr \
    && cmake --build ClangBuildAnalyzer/build -j\
    && cmake --install ClangBuildAnalyzer/build \
    && rm -rf ClangBuildAnalyzer \
    && ClangBuildAnalyzer --version

# Build and install MEOS (required by MEOS plugin)
# Avoid git clone (which may require credentials in some environments). Prefer public tarballs.
ARG MEOS_REF=""
ARG MEOS_TARBALL_URL=""
RUN set -eux; \
    mkdir -p /tmp; cd /tmp; \
    # Determine reference list to try
    if [ -n "${MEOS_TARBALL_URL}" ]; then \
      echo "Fetching MEOS from explicit URL: ${MEOS_TARBALL_URL}"; \
      curl -LfsS -o meos.tar.gz "${MEOS_TARBALL_URL}" || { echo "Failed to download MEOS from MEOS_TARBALL_URL"; exit 1; }; \
      fetched=1; \
    else \
      refs_to_try="${MEOS_REF}"; \
      if [ -z "${refs_to_try}" ]; then refs_to_try="refs/heads/main refs/heads/master"; fi; \
      fetched=0; \
      for ref in $refs_to_try; do \
        for base in "https://codeload.github.com/MobilityDB/MEOS/tar.gz" "https://github.com/MobilityDB/MEOS/archive"; do \
          if [ "${base#*codeload}" != "${base}" ]; then \
            url="$base/$ref"; \
          else \
            url="$base/$ref.tar.gz"; \
          fi; \
          echo "Attempting to fetch MEOS from: $url"; \
          if curl -LfsS -o meos.tar.gz "$url"; then \
            fetched=1; break 2; \
          fi; \
        done; \
      done; \
    fi; \
    if [ "$fetched" -ne 1 ]; then echo "Failed to download MEOS source tarball"; exit 1; fi; \
    tar -xzf meos.tar.gz; \
    meos_src=$(find . -maxdepth 1 -type d -name 'MEOS-*' | head -n 1); \
    cd "$meos_src"; \
    if [ -f CMakeLists.txt ]; then \
      cmake -B build -S . -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local \
      && cmake --build build -j \
      && cmake --install build; \
    else \
      (./autogen.sh || true) \
      && ./configure --prefix=/usr/local \
      && make -j"$(nproc)" \
      && make install; \
    fi; \
    rm -rf /tmp/meos.tar.gz "$meos_src"

# Install GDB Libc++ Pretty Printer
RUN wget -P /usr/share/libcxx/  https://raw.githubusercontent.com/llvm/llvm-project/refs/tags/llvmorg-19.1.7/libcxx/utils/gdb/libcxx/printers.py && \
    cat <<EOF > /etc/gdb/gdbinit
    python
    import sys
    sys.path.insert(0, '/usr/share/libcxx')
    import printers
    printers.register_libcxx_printer_loader()
    end
EOF
