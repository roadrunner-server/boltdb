package boltdb

import (
	"os"
	"testing"

	"tests/helpers"

	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	boltdbPlugin "github.com/roadrunner-server/boltdb/v6"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

const (
	defaultRPC = "127.0.0.1:6001"
	declareRPC = "127.0.0.1:8001"
	jobsErrRPC = "127.0.0.1:8005"

	pipeline = "test-3"

	// rr1db and rr2db are the bolt files the configs point at. They are created
	// in the working directory, so each test removes them on cleanup.
	rr1db = "rr1.db"
	rr2db = "rr2.db"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdbPlugin.Plugin{},
	}
}

// removeDBs deletes the bolt files a test creates. boltdb keeps its queue on
// disk, so a leftover file would carry jobs into the next run.
func removeDBs(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		for _, f := range []string{rr1db, rr2db} {
			_ = os.Remove(f)
		}
	})
}

// bootJobs starts the container with the observed logger and waits for the rpc
// listener.
func bootJobs(t *testing.T, cfgPath, rpcAddr string) (*helpers.RR, func()) {
	t.Helper()

	removeDBs(t)

	return helpers.Start(t, cfgPath, jobsPlugins(),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(rpcAddr),
	)
}

// declarePipe declares the boltdb pipeline the tests push to.
func declarePipe(t *testing.T, rpcAddr, file string) {
	t.Helper()

	client := helpers.NewRPCClient(t, rpcAddr)
	req := &jobsProto.DeclareRequest{Pipeline: map[string]string{
		"driver":      "boltdb",
		"name":        pipeline,
		"prefetch":    "100",
		"permissions": "0755",
		"priority":    "3",
		"file":        file,
	}}

	require.NoError(t, client.Call("jobs.Declare", req, &jobsProto.Empty{}))
}

// TestBoots covers the plain init config.
func TestBoots(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-boltdb-init.yaml", defaultRPC)

	rr.WaitLog(t, "pipeline was started", 1)
}

// TestNoGlobalSection covers a config with no boltdb section: the driver has
// nothing to configure but the container still comes up.
func TestNoGlobalSection(t *testing.T) {
	removeDBs(t)

	helpers.Start(t, "configs/.rr-no-global.yaml", jobsPlugins(), helpers.WithTCPProbe(defaultRPC))
}

// TestAutoAck pushes with auto-ack set, so the driver acknowledges each message
// itself rather than waiting for the worker.
func TestAutoAck(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-boltdb-init.yaml", defaultRPC)

	helpers.PushToPipe("test-1", true, defaultRPC)(t)
	helpers.PushToPipe("test-2", true, defaultRPC)(t)

	rr.RequireLogCount(t, "auto ack is turned on, message acknowledged", 2)
}

// TestPushAndProcess declares a pipeline and follows one job through it.
func TestPushAndProcess(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-boltdb-declare.yaml", declareRPC)

	declarePipe(t, declareRPC, rr1db)
	helpers.ResumePipes(declareRPC, pipeline)(t)
	helpers.PushToPipe(pipeline, false, declareRPC)(t)

	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.WaitLog(t, "job processing was started", 1)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(declareRPC, pipeline)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.DestroyPipelines(declareRPC, pipeline)(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestPriorityQueue covers the config that pushes through the priority queue.
func TestPriorityQueue(t *testing.T) {
	rr, stop := bootJobs(t, "configs/.rr-boltdb-pq.yaml", defaultRPC)

	helpers.PushToPipe("test-1-pq", false, defaultRPC)(t)

	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.WaitLog(t, "job processing was started", 1)

	stop()
	rr.WaitLog(t, "boltdb listener stopped", 1)
}

// TestStatsReportDelayedAndDrained pushes a plain and a delayed job while the
// pipeline is paused, then polls the state rather than sleeping out the delay.
func TestStatsReportDelayedAndDrained(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-boltdb-declare.yaml", declareRPC)

	declarePipe(t, declareRPC, rr1db)
	helpers.ResumePipes(declareRPC, pipeline)(t)
	helpers.PushToPipe(pipeline, false, declareRPC)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(declareRPC, pipeline)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.PushToPipeDelayed(declareRPC, pipeline, 2)(t)
	helpers.PushToPipe(pipeline, false, declareRPC)(t)

	delayed := helpers.WaitStats(t, declareRPC, func(s *jobState.State) bool {
		return s.Delayed == 1 && s.Active == 1
	})

	require.Equal(t, pipeline, delayed.Pipeline)
	require.Equal(t, "boltdb", delayed.Driver)
	require.Equal(t, "push", delayed.Queue)
	require.Equal(t, uint64(3), delayed.Priority)
	require.False(t, delayed.Ready)

	helpers.ResumePipes(declareRPC, pipeline)(t)

	drained := helpers.WaitStats(t, declareRPC, func(s *jobState.State) bool {
		return s.Delayed == 0 && s.Active == 0 && s.Reserved == 0
	})

	require.Equal(t, pipeline, drained.Pipeline)
	require.Equal(t, "boltdb", drained.Driver)

	helpers.DestroyPipelines(declareRPC, pipeline)(t)
}

// TestProtocolErrorIsReported covers a worker whose answer the jobs protocol
// cannot parse. The old test waited out a flat 25s.
func TestProtocolErrorIsReported(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-boltdb-jobs-err.yaml", jobsErrRPC)

	declarePipe(t, jobsErrRPC, rr1db)
	helpers.ResumePipes(jobsErrRPC, pipeline)(t)
	helpers.PushToPipe(pipeline, false, jobsErrRPC)(t)

	rr.WaitLog(t, "jobs protocol error", 1)

	helpers.PausePipelines(jobsErrRPC, pipeline)(t)
	helpers.DestroyPipelines(jobsErrRPC, pipeline)(t)
}
