# 1. Build
make cdk-erigon && echo "[✔] Successfully built cdk-erigon binary"

# 2. Copy binary into container
docker cp ./build/bin/cdk-erigon cdk-erigon-sequencer-001--ac865401432d44f996b3b05151788ba3:/usr/local/bin/cdk-erigon \
  && echo "[✔] Successfully copied binary into container"

# 3. Grant execution permission
docker exec -it cdk-erigon-sequencer-001--ac865401432d44f996b3b05151788ba3 chmod +x /usr/local/bin/cdk-erigon \
  && echo "[✔] Granted executable permission to binary"

## 4. Kill existing process
#docker exec -it cdk-erigon-sequencer-001--ac865401432d44f996b3b05151788ba3 pkill cdk-erigon \
#  && echo "[✔] Terminated existing cdk-erigon process"
#
## 5. Start new process
#docker exec -it cdk-erigon-sequencer-001--ac865401432d44f996b3b05151788ba3 \
#  /usr/local/share/proc-runner/proc-runner.sh /usr/local/bin/cdk-erigon --config /etc/cdk-erigon/config.yaml \
#  && echo "[✔] Successfully started new cdk-erigon process"