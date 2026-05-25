package boltdb

import (
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"
	mocklogger "tests/mock"

	"connectrpc.com/connect"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	kvProto "github.com/roadrunner-server/api-go/v6/kv/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/boltdb/v6"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "google.golang.org/genproto/protobuf/ptype" //nolint:revive,nolintlint
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	rr1db string = "rr1.db"
	rr2db string = "rr2.db"
)

func TestBoltDBInit(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-boltdb-init.yaml",
		Prefix:  "rr",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)
	helpers.DestroyPipelines("127.0.0.1:6001", "test-1", "test-2")
	stopCh <- struct{}{}
	wg.Wait()

	assert.NoError(t, os.Remove(rr1db))
	assert.NoError(t, os.Remove(rr2db))
}

func TestBoltDBPQ(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.2.0",
		Path:    "configs/.rr-boltdb-pq.yaml",
		Prefix:  "rr",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)
	for range 10 {
		t.Run("PushPipeline", helpers.PushToPipe("test-1-pq", false, "127.0.0.1:6001"))
		t.Run("PushPipeline", helpers.PushToPipe("test-2-pq", false, "127.0.0.1:6001"))
	}

	time.Sleep(time.Second)

	helpers.DestroyPipelines("127.0.0.1:6001", "test-1-pq", "test-2-pq")
	stopCh <- struct{}{}
	wg.Wait()

	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was started").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("pipeline was stopped").Len())
	assert.Equal(t, 20, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 4, oLogger.FilterMessageSnippet("boltdb listener stopped").Len())

	assert.NoError(t, os.Remove(rr1db))
	assert.NoError(t, os.Remove(rr2db))
}

func TestBoltDBAutoAck(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-boltdb-init.yaml",
		Prefix:  "rr",
	}

	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)
	t.Run("PushPipeline", helpers.PushToPipe("test-1", true, "127.0.0.1:6001"))
	t.Run("PushPipeline", helpers.PushToPipe("test-2", true, "127.0.0.1:6001"))
	time.Sleep(time.Second)
	helpers.DestroyPipelines("test-1", "test-2")
	stopCh <- struct{}{}
	wg.Wait()

	assert.NoError(t, os.Remove(rr1db))
	assert.NoError(t, os.Remove(rr2db))

	require.Equal(t, 2, oLogger.FilterMessageSnippet("auto ack is turned on, message acknowledged").Len())
}

func TestBoltDBDeclare(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-boltdb-declare.yaml",
		Prefix:  "rr",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)

	t.Run("DeclarePipeline", declareBoltDBPipe("127.0.0.1:8001", rr1db))
	t.Run("ConsumePipeline", helpers.ResumePipes("127.0.0.1:8001", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:8001"))
	time.Sleep(time.Second)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:8001", "test-3"))
	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:8001", "test-3"))
	time.Sleep(time.Second * 5)

	helpers.DestroyPipelines("127.0.0.1:8001", "test-3")
	stopCh <- struct{}{}
	wg.Wait()
	assert.NoError(t, os.Remove(rr1db))
}

func TestBoltDBJobsError(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-boltdb-jobs-err.yaml",
		Prefix:  "rr",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)

	t.Run("DeclarePipeline", declareBoltDBPipe("127.0.0.1:8005", rr1db))
	t.Run("ConsumePipeline", helpers.ResumePipes("127.0.0.1:8005", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:8005"))
	time.Sleep(time.Second * 25)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:8005", "test-3"))
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:8005", "test-3"))
	time.Sleep(time.Second * 5)

	helpers.DestroyPipelines("127.0.0.1:8005", "test-3")
	stopCh <- struct{}{}
	wg.Wait()

	t.Cleanup(func() {
		assert.NoError(t, os.Remove(rr1db))
	})
}

func TestBoltDBNoGlobalSection(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-no-global.yaml",
		Prefix:  "rr",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	_, err = cont.Serve()
	require.NoError(t, err)
	_ = cont.Stop()
}

func TestBoltDBStats(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-boltdb-declare.yaml",
		Prefix:  "rr",
	}

	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)

	t.Run("DeclarePipeline", declareBoltDBPipe("127.0.0.1:8001", rr1db))
	t.Run("ConsumePipeline", helpers.ResumePipes("127.0.0.1:8001", "test-3"))
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:8001"))
	time.Sleep(time.Second * 2)
	t.Run("PausePipeline", helpers.PausePipelines("127.0.0.1:8001", "test-3"))
	time.Sleep(time.Second * 2)
	t.Run("PushPipeline", helpers.PushToPipe("test-3", false, "127.0.0.1:8001"))
	t.Run("PushPipelineDelayed", helpers.PushToPipeDelayed("127.0.0.1:8001", "test-3", 5))

	out := &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:8001", out))

	assert.Equal(t, "test-3", out.Pipeline)
	assert.Equal(t, "boltdb", out.Driver)
	assert.Equal(t, "push", out.Queue)
	assert.Equal(t, uint64(3), out.Priority)

	assert.Equal(t, int64(1), out.Active)
	assert.Equal(t, int64(1), out.Delayed)
	assert.Equal(t, int64(0), out.Reserved)
	assert.Equal(t, false, out.Ready)
	assert.Equal(t, uint64(3), out.Priority)

	time.Sleep(time.Second)
	t.Run("ResumePipeline", helpers.ResumePipes("127.0.0.1:8001", "test-3"))
	time.Sleep(time.Second * 7)

	out = &jobState.State{}
	t.Run("Stats", helpers.Stats("127.0.0.1:8001", out))

	assert.Equal(t, "test-3", out.Pipeline)
	assert.Equal(t, "boltdb", out.Driver)
	assert.Equal(t, "push", out.Queue)

	assert.Equal(t, int64(0), out.Active)
	assert.Equal(t, int64(0), out.Delayed)
	assert.Equal(t, int64(0), out.Reserved)
	assert.Equal(t, true, out.Ready)

	time.Sleep(time.Second)
	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:8001", "test-3"))

	time.Sleep(time.Second * 5)
	stopCh <- struct{}{}
	wg.Wait()

	t.Cleanup(func() {
		assert.NoError(t, os.Remove(rr1db))
	})
}

func TestBoltDBOTEL(t *testing.T) {
	// otel/v6 (≥beta.3) hard-rejects the zipkin exporter at Init
	// (plugin.go:89 returns errors.Errorf("zipkin exporter is deprecated")).
	// The config + test verification still target zipkin (/api/v2/spans).
	// Skip until upstream restores zipkin or the test migrates to OTLP+jaeger.
	t.Skip("blocked on otel/v6 hard-rejecting zipkin exporter; config + verification still target zipkin")
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.1.0",
		Path:    "configs/.rr-boltdb-otel.yaml",
		Prefix:  "rr",
	}

	tracer := mocklogger.NewInMemoryTracer(t)
	l, oLogger := mocklogger.SlogTestLogger(slog.LevelDebug)
	err := cont.RegisterAll(
		cfg,
		&server.Plugin{},
		tracer,
		&rpcPlugin.Plugin{},
		l,
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&boltdb.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	if err != nil {
		t.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 3)
	t.Run("PushPipeline", helpers.PushToPipe("test-1", false, "127.0.0.1:6001"))
	time.Sleep(time.Second)

	t.Run("DestroyPipeline", helpers.DestroyPipelines("127.0.0.1:6001", "test-1"))
	stopCh <- struct{}{}
	wg.Wait()

	spans := tracer.Exp.GetSpans()
	spanNameSet := make(map[string]struct{}, len(spans))
	for _, s := range spans {
		spanNameSet[s.Name] = struct{}{}
	}

	uniqueNames := make([]string, 0, len(spanNameSet))
	for name := range spanNameSet {
		uniqueNames = append(uniqueNames, name)
	}
	slices.Sort(uniqueNames)

	expected := []string{
		"boltdb_listener",
		"boltdb_push",
		"boltdb_stop",
		"destroy_pipeline",
		"jobs_listener",
		"push",
	}
	assert.Equal(t, expected, uniqueNames)

	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was pushed successfully").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job processing was started").Len())
	assert.Equal(t, 1, oLogger.FilterMessageSnippet("job was processed successfully").Len())
	assert.Equal(t, 2, oLogger.FilterMessageSnippet("boltdb listener stopped").Len())

	t.Cleanup(func() {
		assert.NoError(t, os.Remove("rr-otel.db"))
	})
}

func TestBoltDb(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.1.0",
		Path:    "configs/.rr-boltdb.yaml",
		Prefix:  "rr",
	}

	err := cont.RegisterAll(
		cfg,
		&kv.Plugin{},
		&boltdb.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
		&memory.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	}()

	time.Sleep(time.Second * 1)
	t.Run("BOLTDB", testRPCMethods)
	stopCh <- struct{}{}
	wg.Wait()

	_ = os.Remove("rr.db")
}

func testRPCMethods(t *testing.T) {
	const storage = "boltdb-rr"

	client := helpers.NewKVClient(t, "127.0.0.1:6001")
	ctx := t.Context()

	tt := durationpb.New(time.Second * 5)
	keys := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a"},
			{Key: "b"},
			{Key: "c"},
		},
	}

	data := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa")},
			{Key: "b", Value: []byte("bb")},
			{Key: "c", Value: []byte("cc"), Ttl: tt},
			{Key: "d", Value: []byte("dd")},
			{Key: "e", Value: []byte("ee")},
		},
	}

	_, err := client.Set(ctx, connect.NewRequest(data))
	assert.NoError(t, err)

	resp, err := client.Has(ctx, connect.NewRequest(keys))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 3)

	// key "c" should be deleted
	time.Sleep(time.Second * 7)

	resp, err = client.Has(ctx, connect.NewRequest(keys))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 2)

	resp, err = client.MGet(ctx, connect.NewRequest(keys))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 2) // c is expired

	tt2 := durationpb.New(time.Second * 10)

	data2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Ttl: tt2},
			{Key: "b", Ttl: tt2},
			{Key: "d", Ttl: tt2},
		},
	}

	_, err = client.MExpire(ctx, connect.NewRequest(data2))
	assert.NoError(t, err)

	keys2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a"},
			{Key: "b"},
			{Key: "d"},
		},
	}

	resp, err = client.TTL(ctx, connect.NewRequest(keys2))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 3)

	// HAS AFTER TTL
	time.Sleep(time.Second * 15)
	resp, err = client.Has(ctx, connect.NewRequest(keys2))
	assert.NoError(t, err)
	assert.Empty(t, resp.Msg.GetItems())

	keysDel := &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "e"}},
	}

	_, err = client.Delete(ctx, connect.NewRequest(keysDel))
	assert.NoError(t, err)

	resp, err = client.Has(ctx, connect.NewRequest(keysDel))
	assert.NoError(t, err)
	assert.Empty(t, resp.Msg.GetItems())

	dataClear := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa")},
			{Key: "b", Value: []byte("bb")},
			{Key: "c", Value: []byte("cc")},
			{Key: "d", Value: []byte("dd")},
			{Key: "e", Value: []byte("ee")},
		},
	}

	_, err = client.Set(ctx, connect.NewRequest(dataClear))
	assert.NoError(t, err)

	resp, err = client.Has(ctx, connect.NewRequest(dataClear))
	assert.NoError(t, err)
	assert.Len(t, resp.Msg.GetItems(), 5)

	_, err = client.Clear(ctx, connect.NewRequest(&kvProto.KvRequest{Storage: storage}))
	assert.NoError(t, err)

	resp, err = client.Has(ctx, connect.NewRequest(dataClear))
	assert.NoError(t, err)
	assert.Empty(t, resp.Msg.GetItems())
}

func declareBoltDBPipe(address string, file string) func(t *testing.T) {
	return func(t *testing.T) {
		client := helpers.NewJobsClient(t, address)
		req := &jobsProto.DeclareRequest{Pipeline: map[string]string{
			"driver":      "boltdb",
			"name":        "test-3",
			"prefetch":    "100",
			"permissions": "0755",
			"priority":    "3",
			"file":        file,
		}}
		_, err := client.Declare(t.Context(), connect.NewRequest(req))
		assert.NoError(t, err)
	}
}
