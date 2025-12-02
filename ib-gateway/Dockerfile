
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV IBC_VERSION=3.16.0
ENV TWS_VERSION=1010
ENV IBC_HOME=/opt/ibc

# -----------------------------
# Install dependencies
# -----------------------------
RUN apt-get update && apt-get install -y \
    wget unzip openjdk-17-jre xvfb x11vnc \
    libxpm4 libxrender1 libxtst6 libxi6 libxrandr2 libfreetype6 \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Install IB Gateway
# -----------------------------
RUN mkdir -p /opt/ib && \
    wget -q https://download2.interactivebrokers.com/portal/gateway-linux-${TWS_VERSION}-x64.sh -O /opt/ib/installer.sh && \
    chmod +x /opt/ib/installer.sh && \
    /opt/ib/installer.sh -q && \
    rm /opt/ib/installer.sh

# -----------------------------
# Install IBC
# -----------------------------
RUN mkdir -p ${IBC_HOME} && \
    wget -q https://github.com/IbcAlpha/IBC/releases/download/${IBC_VERSION}/IBC.zip -O /tmp/IBC.zip && \
    unzip /tmp/IBC.zip -d ${IBC_HOME} && \
    rm /tmp/IBC.zip

# -----------------------------
# Copy config
# -----------------------------
COPY ibc/config.ini ${IBC_HOME}/config.ini

# -----------------------------
# Expose ports
# -----------------------------
EXPOSE 4001 4002

# -----------------------------
# Start script
# -----------------------------
COPY start.sh /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]
