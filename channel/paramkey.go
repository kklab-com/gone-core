package channel

import (
	"sync"
)

type ParamKey string

const ParamAcceptTimeout = ParamKey("accept_timeout")
const ParamReadBufferSize = ParamKey("read_buffer_size")
const ParamReadTimeout = ParamKey("read_timeout")
const ParamWriteTimeout = ParamKey("write_timeout")

func GetParamIntDefault(ch Channel, key ParamKey, defaultValue int) int {
	switch v := ch.Param(key).(type) {
	case int8:
		return int(v)
	case uint8:
		return int(v)
	case int16:
		return int(v)
	case uint16:
		return int(v)
	case int32:
		return int(v)
	case int:
		return v
	}

	return defaultValue
}

func GetParamInt64Default(ch Channel, key ParamKey, defaultValue int64) int64 {
	switch v := ch.Param(key).(type) {
	case int8:
		return int64(v)
	case uint8:
		return int64(v)
	case int16:
		return int64(v)
	case uint16:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	case int:
		return int64(v)
	case uint:
		return int64(v)
	case int64:
		return v
	}

	return defaultValue
}

func GetParamStringDefault(ch Channel, key ParamKey, defaultValue string) string {
	switch v := ch.Param(key).(type) {
	case string:
		return v
	}

	return defaultValue
}

func GetParamBoolDefault(ch Channel, key ParamKey, defaultValue bool) bool {
	switch v := ch.Param(key).(type) {
	case bool:
		return v
	}

	return defaultValue
}

type Params struct {
	sync.Map
}

func (p *Params) Load(key ParamKey) (value interface{}, ok bool) {
	return p.Map.Load(key)
}

func (p *Params) Store(key ParamKey, value interface{}) {
	p.Map.Store(key, value)
}
func (p *Params) Range(f func(key ParamKey, value interface{}) bool) {
	p.Map.Range(func(k, v interface{}) bool {
		return f(k.(ParamKey), v)
	})
}
func (p *Params) Delete(key ParamKey) {
	p.Map.Delete(key)
}
func (p *Params) LoadOrStore(key ParamKey, value interface{}) (actual interface{}, loaded bool) {
	return p.Map.LoadOrStore(key, value)
}
