package boltdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testLog struct{}

func (tl *testLog) NamedLogger(string) *zap.Logger {
	log, _ := zap.NewDevelopment()
	return log
}

func TestPluginInit(t *testing.T) {
	p := Plugin{}
	require.Error(t, p.Init(&testLog{}, &Cfg{}))
}

func TestPluginName(t *testing.T) {
	p := Plugin{}
	require.Equal(t, "boltdb", p.Name())
}

type Cfg struct{}

func (c *Cfg) UnmarshalKey(string, any) error {
	return nil
}

func (c *Cfg) Unmarshal(any) error {
	return nil
}

func (c *Cfg) Get(string) any {
	return nil
}

func (c *Cfg) Overwrite(map[string]any) error {
	return nil
}

func (c *Cfg) Has(string) bool {
	return false
}

func (c *Cfg) GracefulTimeout() time.Duration {
	return time.Duration(0)
}

func (c *Cfg) RRVersion() string {
	return "2.7.0"
}
