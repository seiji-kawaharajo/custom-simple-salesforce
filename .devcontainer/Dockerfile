FROM mcr.microsoft.com/devcontainers/python:1-3.13-bullseye

RUN <<EOF
  apt-get update
  apt-get install -y postgresql-client
  pip install --upgrade pip
  pip install uv
  rm -rf /var/lib/apt/lists/*
EOF

ENV TZ=Asia/Tokyo