package boltdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var noopLog = zap.NewNop() //nolint:gochecknoglobals

func TestPluginInit(t *testing.T) {
	p := Plugin{}
	require.NoError(t, p.Init(noopLog, &Cfg{}))
}

func TestPluginName(t *testing.T) {
	p := Plugin{}
	require.Equal(t, "boltdb", p.Name())
}

type Cfg struct{}

func (c *Cfg) UnmarshalKey(name string, out interface{}) error {
	return nil
}

func (c *Cfg) Unmarshal(out interface{}) error {
	return nil
}

func (c *Cfg) Get(name string) interface{} {
	return nil
}

func (c *Cfg) Overwrite(values map[string]interface{}) error {
	return nil
}

func (c *Cfg) Has(name string) bool {
	return false
}

func (c *Cfg) GracefulTimeout() time.Duration {
	return time.Duration(0)
}

func (c *Cfg) RRVersion() string {
	return "2.7.0"
}
