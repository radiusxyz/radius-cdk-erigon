make cdk-erigon

docker cp ./build/bin/cdk-erigon cdk-erigon-sequencer-001:/usr/local/bin/cdk-erigon

docker exec -it cdk-erigon-sequencer-001 chmod +x /usr/local/bin/cdk-erigon

docker exec -it cdk-erigon-sequencer-001 pkill cdk-erigon

docker exec -it cdk-erigon-sequencer-001 \
  /usr/local/share/proc-runner/proc-runner.sh cdk-erigon --config /etc/cdk-erigon/config.yaml