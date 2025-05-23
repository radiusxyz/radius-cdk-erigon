make cdk-erigon && echo "[✔] Successfully built cdk-erigon binary"

kurtosis files copy ./build/bin/cdk-erigon cdk cdk-erigon-sequencer-001:/usr/local/bin/cdk-erigon \
  && echo "[✔] Successfully copied binary into container"

kurtosis service exec cdk cdk-erigon-sequencer-001 "chmod +x /usr/local/bin/cdk-erigon" \
  && echo "[✔] Granted executable permission to binary"

kurtosis service exec cdk cdk-erigon-sequencer-001 "pkill -SIGTRAP \"proc-runner.sh\"" \
  && echo "[✔] Terminated existing cdk-erigon process"

sleep 1

kurtosis service exec cdk cdk-erigon-sequencer-001 "pkill -SIGINT \"cdk-erigon\"" \
  && echo "[✔] Successfully started new cdk-erigon process"