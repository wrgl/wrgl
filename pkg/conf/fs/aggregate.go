package conffs

import (
	"reflect"

	"github.com/imdario/mergo"
	"github.com/wrgl/wrgl/pkg/conf"
)

type ptrTransformer struct {
}

func (t *ptrTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	if typ.Kind() == reflect.Ptr && typ.Elem().Kind() != reflect.Struct {
		return func(dst, src reflect.Value) error {
			if dst.CanSet() && !src.IsNil() {
				dst.Set(src)
			}
			return nil
		}
	}
	return nil
}

func (s *Store) aggregateConfig() (*conf.Config, error) {
	localConfig, err := s.readConfig(localPath(s.rootDir))
	if err != nil {
		return nil, err
	}
	fp, err := globalConfigPath()
	if err != nil {
		return nil, err
	}
	globalConfig, err := s.readConfig(fp)
	if err != nil {
		return nil, err
	}
	sysConfig, err := s.readConfig(systemConfigPath())
	if err != nil {
		return nil, err
	}
	err = mergo.Merge(globalConfig, localConfig, mergo.WithOverride, mergo.WithTransformers(&ptrTransformer{}))
	if err != nil {
		return nil, err
	}
	err = mergo.Merge(sysConfig, globalConfig, mergo.WithOverride, mergo.WithTransformers(&ptrTransformer{}))
	if err != nil {
		return nil, err
	}
	return sysConfig, nil
}
