package boltdb

import (
	"net/rpc"
	"testing"
	"time"

	"tests/helpers"

	kvProto "github.com/roadrunner-server/api-go/v6/kv/v1"
	boltdbPlugin "github.com/roadrunner-server/boltdb/v6"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/stretchr/testify/require"
)

const (
	kvStorage = "boltdb-rr"
	// shortTTL is the lifetime given to keys expected to expire during a test.
	shortTTL   = time.Second * 2
	expiryWait = time.Second * 30
	expiryTick = time.Millisecond * 250
)

// bootKV starts the container and hands back a connected rpc client with the
// storage emptied, so each test begins from a known state.
func bootKV(t *testing.T) *rpc.Client {
	t.Helper()

	removeDBs(t)

	helpers.Start(t, "configs/.rr-boltdb.yaml",
		[]any{&kv.Plugin{}, &boltdbPlugin.Plugin{}, &rpcPlugin.Plugin{}, &memory.Plugin{}},
		helpers.WithTCPProbe(defaultRPC),
	)

	client := helpers.NewRPCClient(t, defaultRPC)
	require.NoError(t, client.Call("kv.Clear", &kvProto.Request{Storage: kvStorage}, &kvProto.Response{}))

	return client
}

func kvItems(pairs map[string]string) *kvProto.Request {
	req := &kvProto.Request{Storage: kvStorage}
	for k, v := range pairs {
		req.Items = append(req.Items, &kvProto.Item{Key: k, Value: []byte(v)})
	}
	return req
}

func kvKeys(names ...string) *kvProto.Request {
	req := &kvProto.Request{Storage: kvStorage}
	for _, n := range names {
		req.Items = append(req.Items, &kvProto.Item{Key: n})
	}
	return req
}

// kvHas returns how many of the given keys the storage currently holds.
func kvHas(t *testing.T, client *rpc.Client, names ...string) int {
	t.Helper()

	resp := &kvProto.Response{}
	require.NoError(t, client.Call("kv.Has", kvKeys(names...), resp))

	return len(resp.GetItems())
}

func TestKVSetAndHas(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", kvItems(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))

	require.Equal(t, 2, kvHas(t, client, "a", "b"))
	require.Equal(t, 0, kvHas(t, client, "missing"))
}

// TestKVMGetReturnsStoredValues checks MGet hands back the bytes that were set,
// not merely the keys.
func TestKVMGetReturnsStoredValues(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", kvItems(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))

	resp := &kvProto.Response{}
	require.NoError(t, client.Call("kv.MGet", kvKeys("a", "b", "absent"), resp))

	got := make(map[string]string, len(resp.GetItems()))
	for _, it := range resp.GetItems() {
		got[it.GetKey()] = string(it.GetValue())
	}

	require.Equal(t, map[string]string{"a": "aa", "b": "bb"}, got)
}

func TestKVDeleteRemovesOnlyTheNamedKey(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", kvItems(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))
	require.NoError(t, client.Call("kv.Delete", kvKeys("a"), &kvProto.Response{}))

	require.Equal(t, 0, kvHas(t, client, "a"))
	require.Equal(t, 1, kvHas(t, client, "b"))
}

func TestKVClearEmptiesTheStorage(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", kvItems(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))
	require.NoError(t, client.Call("kv.Clear", &kvProto.Request{Storage: kvStorage}, &kvProto.Response{}))

	require.Equal(t, 0, kvHas(t, client, "a", "b"))
}

// TestKVTTLReportsRemainingLifetime checks a key with a TTL reports one and a
// key without does not.
func TestKVTTLReportsRemainingLifetime(t *testing.T) {
	client := bootKV(t)

	req := &kvProto.Request{
		Storage: kvStorage,
		Items: []*kvProto.Item{
			{Key: "permanent", Value: []byte("v")},
			{Key: "ephemeral", Value: []byte("v"), Timeout: time.Now().UTC().Add(time.Minute).Format(time.RFC3339)},
		},
	}
	require.NoError(t, client.Call("kv.Set", req, &kvProto.Response{}))

	resp := &kvProto.Response{}
	require.NoError(t, client.Call("kv.TTL", kvKeys("permanent", "ephemeral"), resp))

	require.Len(t, resp.GetItems(), 1)
	require.Equal(t, "ephemeral", resp.GetItems()[0].GetKey())
}

// TestKVKeyExpiresAfterTTL polls for the expiry rather than sleeping, and checks
// the key without a TTL survives it.
func TestKVKeyExpiresAfterTTL(t *testing.T) {
	client := bootKV(t)

	req := &kvProto.Request{
		Storage: kvStorage,
		Items: []*kvProto.Item{
			{Key: "permanent", Value: []byte("v")},
			{Key: "ephemeral", Value: []byte("v"), Timeout: time.Now().UTC().Add(shortTTL).Format(time.RFC3339)},
		},
	}
	require.NoError(t, client.Call("kv.Set", req, &kvProto.Response{}))
	require.Equal(t, 2, kvHas(t, client, "permanent", "ephemeral"))

	require.Eventually(t, func() bool {
		return kvHas(t, client, "ephemeral") == 0
	}, expiryWait, expiryTick, "the key with a TTL never expired")

	require.Equal(t, 1, kvHas(t, client, "permanent"), "the key without a TTL must survive")
}

func TestKVMExpireAppliesTTLToExistingKeys(t *testing.T) {
	client := bootKV(t)

	require.NoError(t, client.Call("kv.Set", kvItems(map[string]string{"a": "aa", "b": "bb"}), &kvProto.Response{}))

	expire := &kvProto.Request{
		Storage: kvStorage,
		Items: []*kvProto.Item{
			{Key: "a", Timeout: time.Now().UTC().Add(shortTTL).Format(time.RFC3339)},
			{Key: "b", Timeout: time.Now().UTC().Add(shortTTL).Format(time.RFC3339)},
		},
	}
	require.NoError(t, client.Call("kv.MExpire", expire, &kvProto.Response{}))

	require.Eventually(t, func() bool {
		return kvHas(t, client, "a", "b") == 0
	}, expiryWait, expiryTick, "keys did not expire after MExpire")
}

func TestKVUnknownStorageIsRejected(t *testing.T) {
	client := bootKV(t)

	err := client.Call("kv.Has", &kvProto.Request{
		Storage: "not-configured",
		Items:   []*kvProto.Item{{Key: "a"}},
	}, &kvProto.Response{})

	require.Error(t, err)
}
