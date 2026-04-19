# Use Amazon Linux 2023 (same as Lambda Python 3.12 runtime)
FROM public.ecr.aws/amazonlinux/amazonlinux:2023

# Install dependencies
RUN dnf update -y && \
    dnf install -y \
        tar \
        xz \
        gzip \
        unzip \
        wget \
        make \
        gcc \
        gcc-c++ \
        nasm \
        yasm \
        pkgconfig \
        zlib-devel \
        bzip2 \
        bzip2-devel \
        xz-devel \
        libffi-devel \
        openssl-devel \
        && dnf clean all

WORKDIR /tmp

# Download static FFmpeg build (much simpler than compiling)
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz && \
    tar -xvf ffmpeg-release-amd64-static.tar.xz

# Create Lambda layer structure
RUN mkdir -p /opt/bin

# Copy ffmpeg and ffprobe to layer
RUN cp ffmpeg-*-static/ffmpeg /opt/bin/ && \
    cp ffmpeg-*-static/ffprobe /opt/bin/

# Ensure executables
RUN chmod +x /opt/bin/ffmpeg /opt/bin/ffprobe

# Package the layer
WORKDIR /opt
RUN zip -r9 /tmp/ffmpeg-layer.zip .

# Final output
CMD ["cp", "/tmp/ffmpeg-layer.zip", "/output/"]