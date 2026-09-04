# The Base Dockerfile installs and configures all relevant tooling to build the dependencies and NebulaStream.
# Currently our compiler toolchain is based on llvm-18 using libc++.
# Additionally we install a recent CMake version and the mold linker.
FROM ubuntu:24.04

ARG LLVM_TOOLCHAIN_VERSION=19
ARG MOLD_VERSION=2.37.1
ARG CMAKE_VERSION=3.31.6
ENV LLVM_TOOLCHAIN_VERSION=${LLVM_TOOLCHAIN_VERSION}
ENV CMAKE_VERSION=${CMAKE_VERSION}

RUN apt update -y && apt install \
    wget \
    zip \
    unzip \
    tar \
    curl \
    gpg \
    git \
    ca-certificates \
    linux-libc-dev \
    build-essential \
    g++-14 \
    make \
    libssl-dev \
    openssl \
    ccache \
    ninja-build \
    pkg-config \
    bison \
    pipx \
    -y

# install llvm based toolchain
RUN curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key | gpg --dearmor -o /etc/apt/keyrings/llvm-snapshot.gpg \
    && chmod a+r /etc/apt/keyrings/llvm-snapshot.gpg \
    && echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" > /etc/apt/sources.list.d/llvm-snapshot.list \
    && echo "deb-src [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/llvm-snapshot.gpg] http://apt.llvm.org/"$(. /etc/os-release && echo "$VERSION_CODENAME")"/ llvm-toolchain-"$(. /etc/os-release && echo "$VERSION_CODENAME")"-${LLVM_TOOLCHAIN_VERSION} main" >> /etc/apt/sources.list.d/llvm-snapshot.list \
    && apt update -y && apt install clang-${LLVM_TOOLCHAIN_VERSION} libc++-${LLVM_TOOLCHAIN_VERSION}-dev libc++abi-${LLVM_TOOLCHAIN_VERSION}-dev libclang-rt-${LLVM_TOOLCHAIN_VERSION}-dev -y

# install recent version of the mold linker
RUN wget https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz \
    && tar -xf mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz \
    && cp -r mold-${MOLD_VERSION}-$(uname -m)-linux/* /usr \
    && rm -rf mold-${MOLD_VERSION}-$(uname -m)-linux mold-${MOLD_VERSION}-$(uname -m)-linux.tar.gz \
    && mold --version

# install recent version of cmake
RUN wget https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}.tar.gz \
    && tar -xf cmake-${CMAKE_VERSION}.tar.gz \
    && cd cmake-${CMAKE_VERSION} \
    && ./configure --parallel=$(nproc) --prefix=/usr \
    && make install -j $(nproc)\
    && cd .. \
    && rm -rf cmake-${CMAKE_VERSION}.tar.gz cmake-${CMAKE_VERSION} \
    && cmake --version

# Install MEOS/MobilityDB build dependencies
RUN apt update -y && apt install -y \
    postgresql-server-dev-all \
    postgresql-common \
    postgresql-client \
    postgis \
    libproj-dev \
    libjson-c-dev \
    libgsl-dev \
    libgeos-dev \
    libgeos++-dev \
    libgeos-c1v5 \
    libxml2-dev \
    zlib1g-dev \
    libh3-dev \
    libgdal-dev \
    autoconf \
    automake \
    libtool \
    pkg-config

# Build MobilityDB with MEOS and ALL optional families enabled using GCC.
# -DALL=ON mirrors the ecosystem-wide provision-meos recipe (all present and
# future families: CBUFFER/NPOINT/POSE/RGEO/QUADBIN/H3/POINTCLOUD/RASTER/…), so
# the libmeos the NES MEOS operators link against matches CI. POINTCLOUD builds
# without PostgreSQL as of MobilityDB #1370 (cmake pointcloud_libpc, libxml2+zlib).
# Pinned to an upstream master commit for a reproducible dependency image.
RUN git clone https://github.com/MobilityDB/MobilityDB.git /usr/local/src/MobilityDB \
    && cd /usr/local/src/MobilityDB \
    && git checkout 5c5b3cd25b2dba55a137eeacc4b133998cbc6530 \
    && mkdir -p build \
    && cd build \
    && cmake -DMEOS=ON -DALL=ON -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ \
        -DH3_INCLUDE_DIR=/usr/include/h3 \
        -DH3_LIBRARY=/usr/lib/$(gcc -dumpmachine)/libh3.so .. \
    && make -j$(nproc) \
    && make install \
    && ldconfig

# default cmake generator is ninja
ENV CMAKE_GENERATOR=Ninja

# set default compiler to clang and make libc++ available via ldconfig
RUN ln -sf /usr/bin/clang-${LLVM_TOOLCHAIN_VERSION} /usr/bin/cc \
    && ln -sf /usr/bin/clang++-${LLVM_TOOLCHAIN_VERSION} /usr/bin/c++ \
    && update-alternatives --install /usr/bin/cc cc /usr/bin/clang-${LLVM_TOOLCHAIN_VERSION} 30\
    && update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-${LLVM_TOOLCHAIN_VERSION} 30\
    && update-alternatives --auto cc \
    && update-alternatives --auto c++ \
    && update-alternatives --display cc \
    && update-alternatives --display c++ \
    && echo /usr/lib/llvm-${LLVM_TOOLCHAIN_VERSION}/lib > /etc/ld.so.conf.d/libcxx.conf \
    && ldconfig \
    && ls -l /usr/bin/cc /usr/bin/c++ \
    && cc --version \
    && c++ --version
