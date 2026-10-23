// 本文件测试 V1 品种解析的唯一事实规则与拒绝行为。
package v1

import (
	"net/http"
	"testing"
)

// 验证 providerRef、instrumentId、exchange 与目录之间的解析优先级和冲突拒绝。
func TestResolveInstrumentRules(t *testing.T) {
	cases := []struct {
		name    string
		ref     v1InstrumentReference
		wantID  string
		wantErr bool
	}{
		{
			name:   "providerRef 提供路由",
			ref:    v1InstrumentReference{Symbol: "600519", ProviderRef: map[string]any{"kind": "stock", "market": uint8(1)}},
			wantID: "gotdx:stock:1:600519",
		},
		{
			name:   "只有 instrumentId",
			ref:    v1InstrumentReference{ID: "gotdx:index:1:000001"},
			wantID: "gotdx:index:1:000001",
		},
		{
			name: "instrumentId 与 providerRef 一致",
			ref: v1InstrumentReference{ID: "gotdx:index:1:000001", Symbol: "000001",
				ProviderRef: map[string]any{"kind": "index", "market": 1}},
			wantID: "gotdx:index:1:000001",
		},
		{
			name: "instrumentId 与 providerRef 冲突",
			ref: v1InstrumentReference{ID: "gotdx:index:0:000001", Symbol: "000001",
				ProviderRef: map[string]any{"kind": "index", "market": 1}},
			wantErr: true,
		},
		{
			name:   "exchange 推导路由",
			ref:    v1InstrumentReference{Symbol: "000001", Exchange: "SH"},
			wantID: "gotdx:index:1:000001",
		},
		{
			name:   "代码后缀推导交易所",
			ref:    v1InstrumentReference{Symbol: "000001.SZ"},
			wantID: "gotdx:stock:0:000001",
		},
		{
			name:    "指数落在错误市场",
			ref:     v1InstrumentReference{ID: "gotdx:index:0:000001"},
			wantErr: true,
		},
		{
			name:    "providerRef 缺少 kind",
			ref:     v1InstrumentReference{Symbol: "600519", ProviderRef: map[string]any{"market": 1}},
			wantErr: true,
		},
		{
			name: "扩展行情路由",
			ref: v1InstrumentReference{ID: "gotdx:ex:31:HSI", Symbol: "HSI", Exchange: "HK",
				ProviderRef: map[string]any{"kind": "ex", "category": 31}},
			wantID: "gotdx:ex:31:HSI",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolved, apiErr, status := resolveInstrument(tc.ref, nil)
			if tc.wantErr {
				if apiErr == nil || status != http.StatusBadRequest || apiErr.Code != v1CodeInvalidRequest {
					t.Fatalf("resolve = %#v %#v status=%d, want 400 INVALID_REQUEST", resolved, apiErr, status)
				}
				return
			}
			if apiErr != nil || resolved.ID != tc.wantID {
				t.Fatalf("resolve = %#v %#v, want id %q", resolved, apiErr, tc.wantID)
			}
		})
	}
}
