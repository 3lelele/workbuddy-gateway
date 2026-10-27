package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPromptCompatibilityDefaultRewritesSystemAndAssistantText(t *testing.T) {
	t.Cleanup(func() { setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{}) })
	setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{})
	obj := map[string]any{
		"messages": []any{
			map[string]any{"role": "system", "content": "Relative paths resolve from the workspace root; keep the rest."},
			map[string]any{"role": "assistant", "content": []any{
				map[string]any{"type": "text", "text": "Earlier the workspace root was mentioned."},
				map[string]any{"type": "image_url", "image_url": map[string]any{"url": "workspace root"}},
			}},
			map[string]any{"role": "user", "content": "workspace root must stay in user text"},
			map[string]any{"role": "tool", "content": "workspace root must stay in tool output"},
		},
	}
	if modified := normalizePromptCompatibilityMessages(obj); modified != 2 {
		t.Fatalf("modified messages=%d, want 2", modified)
	}
	messages := obj["messages"].([]any)
	if got := messages[0].(map[string]any)["content"]; got != "Relative paths resolve from the workspace base; keep the rest." {
		t.Fatalf("system content=%q", got)
	}
	blocks := messages[1].(map[string]any)["content"].([]any)
	if got := blocks[0].(map[string]any)["text"]; got != "Earlier the workspace base was mentioned." {
		t.Fatalf("assistant text=%q", got)
	}
	if got := blocks[1].(map[string]any)["image_url"].(map[string]any)["url"]; got != "workspace root" {
		t.Fatalf("non-text block changed: %q", got)
	}
	if got := messages[2].(map[string]any)["content"]; got != "workspace root must stay in user text" {
		t.Fatalf("user content changed: %q", got)
	}
	if got := messages[3].(map[string]any)["content"]; got != "workspace root must stay in tool output" {
		t.Fatalf("tool content changed: %q", got)
	}
}

func TestPromptCompatibilityConfigSupportsArbitraryWordsAndDisable(t *testing.T) {
	t.Cleanup(func() { setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{}) })
	enabled := true
	setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{
		Enabled: &enabled,
		Replacements: map[string]string{
			"secret channel phrase": "neutral phrase",
			"secret":                "neutral",
		},
	})
	got, changed := normalizePromptCompatibilityText("secret channel phrase and secret")
	if got != "neutral phrase and neutral" || changed != 2 {
		t.Fatalf("got=%q changed=%d", got, changed)
	}
	if again, changed := normalizePromptCompatibilityText(got); again != got || changed != 0 {
		t.Fatalf("not idempotent: %q changed=%d", again, changed)
	}
	disabled := false
	setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{Enabled: &disabled})
	if got, changed := normalizePromptCompatibilityText("workspace root"); got != "workspace root" || changed != 0 {
		t.Fatalf("disabled rule changed text: %q %d", got, changed)
	}
}

func TestEnsureRuntimeConfigFileAddsDefaultPromptCompatibility(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	original := `{"gateway":{"apiPort":18317,"webPort":18316},"identity":{"enabled":true,"target":"Keep"},"systemPrompt":{"force":"keep"}}`
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ensureRuntimeConfigFile(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["promptCompatibility"]; !ok {
		t.Fatal("promptCompatibility was not added to old config")
	}
	c, err := parseRuntimeConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	if c.PromptCompatibility.Enabled == nil || !*c.PromptCompatibility.Enabled {
		t.Fatal("default promptCompatibility must be enabled")
	}
	if c.PromptCompatibility.Replacements["workspace root"] != "workspace base" {
		t.Fatalf("default replacements=%v", c.PromptCompatibility.Replacements)
	}
	if c.SystemPrompt.Force != "keep" || c.Identity.Target != "Keep" {
		t.Fatal("existing config was overwritten")
	}
}

func TestPromptCompatibilityLeavesLongerRulesFirst(t *testing.T) {
	t.Cleanup(func() { setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{}) })
	enabled := true
	setPromptCompatibilityConfig(runtimePromptCompatibilityConfig{
		Enabled: &enabled,
		Replacements: map[string]string{
			"workspace":      "ws",
			"workspace root": "workspace base",
		},
	})
	got, _ := normalizePromptCompatibilityText("workspace root")
	if got != "ws base" {
		t.Fatalf("longest-first replacement got=%q", got)
	}
	if strings.Contains(got, "workspace root") {
		t.Fatal("longest rule did not run first")
	}
}
