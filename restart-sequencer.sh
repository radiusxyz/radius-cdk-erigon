# 1. Build
make cdk-erigon && echo "[✔] Successfully built cdk-erigon binary"

# 2. Copy binary into container
docker cp ./build/bin/cdk-erigon cdk-erigon-sequencer-001--a7e3437e78364c7ab2a4e5f96628ecc4:/usr/local/bin/cdk-erigon \
  && echo "[✔] Successfully copied binary into container"

# 3. Grant execution permission
docker exec -it cdk-erigon-sequencer-001--a7e3437e78364c7ab2a4e5f96628ecc4 chmod +x /usr/local/bin/cdk-erigon \
  && echo "[✔] Granted executable permission to binary"

# 4. Kill existing process
docker exec -it cdk-erigon-sequencer-001--a7e3437e78364c7ab2a4e5f96628ecc4 pkill cdk-erigon \
  && echo "[✔] Terminated existing cdk-erigon process"

sleep 5

# 5. Start new process
docker exec -it cdk-erigon-sequencer-001--a7e3437e78364c7ab2a4e5f96628ecc4 \
  /usr/local/share/proc-runner/proc-runner.sh /usr/local/bin/cdk-erigon --config /etc/cdk-erigon/config.yaml \
  && echo "[✔] Successfully started new cdk-erigon process"