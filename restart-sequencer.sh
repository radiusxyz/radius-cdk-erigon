make cdk-erigon

docker cp ./build/bin/cdk-erigon cdk-erigon-sequencer-001--2fa28f045b174a42b471b95b9ba6c42e:/usr/local/bin/cdk-erigon

docker exec -it cdk-erigon-sequencer-001--2fa28f045b174a42b471b95b9ba6c42e chmod +x /usr/local/bin/cdk-erigon

docker exec -it cdk-erigon-sequencer-001--2fa28f045b174a42b471b95b9ba6c42e pkill cdk-erigon

docker exec -it cdk-erigon-sequencer-001--2fa28f045b174a42b471b95b9ba6c42e \
  /usr/local/share/proc-runner/proc-runner.sh cdk-erigon --config /etc/cdk-erigon/config.yaml