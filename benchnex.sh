echo run query $1
./bin/run_nexmark.sh \
	-job_id nexmark-Q$1 \
	-executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
	-user_main org.apache.beam.sdk.nexmark.Main \
  -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
  -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
	-user_args "--runner=org.apache.nemo.compiler.frontend.beam.NemoRunner --streaming=true --query=$1 --manageResources=false --monitorJobs=true --streamTimeout=400 --numEventGenerators=1 --numEvents=40000000 --isRateLimited=true --firstEventRate=100000 --nextEventRate=100000 --windowSizeSec=4 --windowPeriodSec=2 --sinkType=KAFKA --kafkaResultsTopic=test --bootstrapServers=localhost:9092"

