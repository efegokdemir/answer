/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package plugin_common

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/apache/answer/internal/base/constant"
	basedata "github.com/apache/answer/internal/base/data"
	"github.com/apache/answer/internal/entity"
	configRepo "github.com/apache/answer/internal/repo/config"
	configService "github.com/apache/answer/internal/service/config"
	"github.com/apache/answer/plugin"
	"github.com/segmentfault/pacman/contrib/cache/memory"
)

func TestUpdatePluginStatusMergesConcurrentReplicaChanges(t *testing.T) {
	baseService, db := newPluginCommonTestService(t, &pluginCommonTestConfigRepo{})
	defer func() {
		if err := db.DB.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	}()
	if _, err := baseService.configService.GetStringValue(context.Background(), constant.PluginStatus); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, slugName := range []string{"replica-a-plugin", "replica-b-plugin"} {
		replicaService := &PluginCommonService{configService: baseService.configService}
		go func(slugName string, service *PluginCommonService) {
			<-start
			results <- service.UpdatePluginStatus(context.Background(), slugName, true)
		}(slugName, replicaService)
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}

	stored, err := baseService.configService.GetStringValueFromDB(context.Background(), constant.PluginStatus)
	if err != nil {
		t.Fatal(err)
	}
	statuses := make(map[string]bool)
	if err := json.Unmarshal([]byte(stored), &statuses); err != nil {
		t.Fatal(err)
	}
	if !statuses["replica-a-plugin"] || !statuses["replica-b-plugin"] {
		t.Fatalf("status map lost a concurrent update: %s", stored)
	}
	cached, err := baseService.configService.GetStringValue(context.Background(), constant.PluginStatus)
	if err != nil {
		t.Fatal(err)
	}
	if cached != stored {
		t.Fatalf("cached status = %s, want latest database value %s", cached, stored)
	}
}

func TestRefreshPluginStatusAndConfigFromDatabase(t *testing.T) {
	registerPluginCommonConfigTestPlugin(t)
	oldStatus, err := plugin.StatusManager.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plugin.StatusManager.UnmarshalJSON(oldStatus) })

	repo := &pluginCommonTestConfigRepo{configs: map[string]string{
		pluginCommonConfigTestSlug: `{"value":"old"}`,
	}}
	service, db := newPluginCommonTestService(t, repo)
	defer func() {
		if err := db.DB.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	}()

	ctx := context.Background()
	if err := plugin.StatusManager.UnmarshalJSON([]byte(`{"plugin-common-config-refresh-test":false}`)); err != nil {
		t.Fatal(err)
	}
	if err := service.refreshPluginConfigs(ctx, true, false); err != nil {
		t.Fatal(err)
	}
	if got := pluginCommonConfigTestInstance.configValue(); got != `{"value":"old"}` {
		t.Fatalf("initial plugin config = %s, want old config", got)
	}

	if err := service.configService.UpdateConfig(ctx, constant.PluginStatus,
		`{"plugin-common-config-refresh-test":true}`); err != nil {
		t.Fatal(err)
	}
	repo.set(pluginCommonConfigTestSlug, `{"value":"new"}`)

	service.refreshPluginStatus(ctx)
	if err := service.refreshPluginConfigs(ctx, false, false); err != nil {
		t.Fatal(err)
	}
	if !plugin.StatusManager.IsEnabled(pluginCommonConfigTestSlug) {
		t.Fatal("plugin status was not refreshed from the database")
	}
	if got := pluginCommonConfigTestInstance.configValue(); got != `{"value":"new"}` {
		t.Fatalf("refreshed plugin config = %s, want new config", got)
	}
}

func newPluginCommonTestService(
	t *testing.T,
	pluginConfigRepo PluginConfigRepo,
) (*PluginCommonService, *basedata.Data) {
	t.Helper()

	db, err := basedata.NewDB(false, &basedata.Database{Driver: "sqlite", Connection: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	data := &basedata.Data{DB: db, Cache: memory.NewCache()}
	if err := db.Sync(new(entity.Config)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Insert(&entity.Config{Key: constant.PluginStatus, Value: `{}`}); err != nil {
		t.Fatal(err)
	}

	repo := configRepo.NewConfigRepo(data)
	service := &PluginCommonService{
		configService:      configService.NewConfigService(repo),
		pluginConfigRepo:   pluginConfigRepo,
		pluginConfigValues: make(map[string]string),
	}
	return service, data
}

const pluginCommonConfigTestSlug = "plugin-common-config-refresh-test"

var (
	pluginCommonConfigTestOnce     sync.Once
	pluginCommonConfigTestInstance = &pluginCommonTestConfigPlugin{}
)

func registerPluginCommonConfigTestPlugin(t *testing.T) {
	t.Helper()
	pluginCommonConfigTestOnce.Do(func() {
		plugin.Register(pluginCommonConfigTestInstance)
	})
}

type pluginCommonTestConfigPlugin struct {
	mu    sync.Mutex
	value string
}

func (p *pluginCommonTestConfigPlugin) Info() plugin.Info {
	return plugin.Info{SlugName: pluginCommonConfigTestSlug}
}

func (p *pluginCommonTestConfigPlugin) ConfigFields() []plugin.ConfigField { return nil }

func (p *pluginCommonTestConfigPlugin) ConfigReceiver(config []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.value = string(config)
	return nil
}

func (p *pluginCommonTestConfigPlugin) configValue() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.value
}

type pluginCommonTestConfigRepo struct {
	mu      sync.RWMutex
	configs map[string]string
}

func (r *pluginCommonTestConfigRepo) SavePluginConfig(
	_ context.Context, slugName, value string,
) error {
	r.set(slugName, value)
	return nil
}

func (r *pluginCommonTestConfigRepo) GetPluginConfigAll(
	context.Context,
) ([]*entity.PluginConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	configs := make([]*entity.PluginConfig, 0, len(r.configs))
	for slugName, value := range r.configs {
		configs = append(configs, &entity.PluginConfig{PluginSlugName: slugName, Value: value})
	}
	return configs, nil
}

func (r *pluginCommonTestConfigRepo) set(slugName, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.configs == nil {
		r.configs = make(map[string]string)
	}
	r.configs[slugName] = value
}
