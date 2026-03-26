package manager

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestManagerInit(t *testing.T) {
	// empty tests, ensures init() does not panic and does not require access to the k8s API server
}

func TestConfigUnmarshal(t *testing.T) {
	// Simulate the JSON config file content
	configJSON := []byte(`{
		"MetricsPrefix": "custom-megamon",
		"AggregationIntervalSeconds": 30,
		"SliceEnabled": true,
		"SliceOwnerMapConfigMapRef": {
			"Namespace": "custom-namespace",
			"Name": "custom-slice-map"
		}
	}`)

	// Set defaults exactly like MustConfigure does
	cfg := Config{
		MetricsPrefix:              "megamon",
		AggregationIntervalSeconds: 10,
		ReportConfigMapRef: types.NamespacedName{
			Namespace: "megamon-system",
			Name:      "megamon-report",
		},
		SliceOwnerMapConfigMapRef: types.NamespacedName{
			Namespace: "megamon-system",
			Name:      "megamon-slice-owner-map",
		},
		SliceEnabled: false,
	}

	// Unmarshal the JSON over the defaults
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal config JSON: %v", err)
	}

	// Verify overrides from JSON
	if cfg.MetricsPrefix != "custom-megamon" {
		t.Errorf("Expected MetricsPrefix to be 'custom-megamon', got '%s'", cfg.MetricsPrefix)
	}
	if cfg.AggregationIntervalSeconds != 30 {
		t.Errorf("Expected AggregationIntervalSeconds to be 30, got %d", cfg.AggregationIntervalSeconds)
	}
	if !cfg.SliceEnabled {
		t.Errorf("Expected SliceEnabled to be true, got %v", cfg.SliceEnabled)
	}

	// Verify the SliceOwnerMapConfigMapRef was properly overridden
	expectedRef := types.NamespacedName{
		Namespace: "custom-namespace",
		Name:      "custom-slice-map",
	}
	if cfg.SliceOwnerMapConfigMapRef != expectedRef {
		t.Errorf("Expected SliceOwnerMapConfigMapRef to be %v, got %v", expectedRef, cfg.SliceOwnerMapConfigMapRef)
	}

	// Verify fields not in JSON retained their defaults
	expectedReportRef := types.NamespacedName{
		Namespace: "megamon-system",
		Name:      "megamon-report",
	}
	if cfg.ReportConfigMapRef != expectedReportRef {
		t.Errorf("Expected ReportConfigMapRef to retain default %v, got %v", expectedReportRef, cfg.ReportConfigMapRef)
	}
}
