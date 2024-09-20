package syncMap

import "sync"

// ReadOnlyMap 读只字典,value 是一个指针
type ReadOnlyMap interface {
	Get(key string) (any, bool)
	Range(f func(key string, value any) bool)
}

// ReadonlyMapWrapper 包装 sync.Map 以实现 ReadOnlyMap 接口
type ReadonlyMapWrapper struct {
	m *sync.Map
}

func (w *ReadonlyMapWrapper) Get(key string) (any, bool) {
	v, ok := w.m.Load(key)
	if !ok {
		return nil, false
	}
	return v, true
}
func (w *ReadonlyMapWrapper) InitMap(source *sync.Map) {
	w.m = source
}

func (w *ReadonlyMapWrapper) Range(f func(key string, value any) bool) {
	w.m.Range(func(k, v interface{}) bool {
		return f(k.(string), v)
	})
}
