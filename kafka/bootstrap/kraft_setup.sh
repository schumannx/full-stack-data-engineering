#!/usr/bin/env bash
# Bootstrap a single-node Kafka broker in KRaft mode (no ZooKeeper) on EC2.
# For a portfolio this single node is plenty; production would run 3 brokers.
# See DESIGN.md §2.7 / PLAN_v2.md §3.
set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
ADVERTISED_HOST="${ADVERTISED_HOST:-$(hostname -f)}"

# --- install (Amazon Linux 2023: java already available via dnf) ---------------
if [ ! -d "$KAFKA_HOME" ]; then
  sudo dnf install -y java-17-amazon-corretto-headless tar gzip
  TARBALL="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  # archive.apache.org permanently hosts every release; downloads.apache.org only
  # serves the current one (older pinned versions 404 once superseded).
  curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}" -o "/tmp/${TARBALL}"
  sudo mkdir -p "$KAFKA_HOME"
  sudo tar -xzf "/tmp/${TARBALL}" -C "$KAFKA_HOME" --strip-components=1
fi

# --- KRaft config --------------------------------------------------------------
CFG="$KAFKA_HOME/config/kraft/server.properties"
sudo sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://${ADVERTISED_HOST}:9092|" "$CFG"
sudo sed -i "s|^log.dirs=.*|log.dirs=/var/lib/kafka-logs|" "$CFG"
sudo mkdir -p /var/lib/kafka-logs

# --- format storage (first run only) -------------------------------------------
if [ ! -f /var/lib/kafka-logs/meta.properties ]; then
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  sudo "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CFG"
  echo "Formatted KRaft storage with cluster id $CLUSTER_ID"
fi

# --- install systemd unit for the broker ---------------------------------------
sudo tee /etc/systemd/system/kafka.service >/dev/null <<EOF
[Unit]
Description=Apache Kafka (KRaft)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${CFG}
ExecStop=${KAFKA_HOME}/bin/kafka-server-stop.sh
Restart=on-failure
RestartSec=5
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now kafka.service
echo "Kafka broker started; advertised at ${ADVERTISED_HOST}:9092"
