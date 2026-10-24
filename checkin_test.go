package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func checkinAt(hour, minute int) time.Time {
	return time.Date(2026, 10, 25, hour, minute, 0, 0, checkinZone)
}

func checkinFixture(t *testing.T) (*dailyCheckinScheduler, *time.Time, checkinTarget) {
	t.Helper()
	chdirTemp(t)
	now := checkinAt(7, 0)
	s := newDailyCheckinScheduler(checkinStateFile)
	s.now = func() time.Time { return now }
	s.random = func(window time.Duration) (time.Duration, error) { return window / 2, nil }
	acc := &Account{Path: "synthetic-checkin.json", Auth: &StoredAuth{
		Edition: "cn", Auth: StoredTokens{AccessToken: "synthetic-checkin-token"},
		Account: StoredAccount{UID: "synthetic-checkin-user"},
	}}
	accountMu.Lock()
	target := checkinTargetLocked(acc)
	oldAccounts := accounts
	accounts = []*Account{acc}
	accountMu.Unlock()
	oldStore := dailyCheckins
	dailyCheckins = s
	t.Cleanup(func() {
		dailyCheckins = oldStore
		accountMu.Lock()
		accounts = oldAccounts
		accountMu.Unlock()
	})
	return s, &now, target
}

func TestCheckinPlanPersistsWithoutStartupOrCredentialEventRequests(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	if jobs := s.planAndClaim(*now, targets, true); len(jobs) != 0 {
		t.Fatal("startup must never dispatch")
	}
	p := *s.doc.Plans[target.key]
	if p.FirstAt != checkinAt(8, 45).UnixMilli() || p.RetryAt != checkinAt(9, 45).UnixMilli() {
		t.Fatalf("wrong windows: %+v", p)
	}
	for _, at := range []time.Time{checkinAt(7, 30), checkinAt(8, 0), checkinAt(8, 44)} {
		if jobs := s.planAndClaim(at, targets, false); len(jobs) != 0 {
			t.Fatal("polling an account change triggered early checkin")
		}
	}
	restarted := newDailyCheckinScheduler(s.file)
	if jobs := restarted.planAndClaim(checkinAt(8, 0), targets, true); len(jobs) != 0 {
		t.Fatal("restart sent a request")
	}
	if got := restarted.doc.Plans[target.key]; got.FirstAt != p.FirstAt || got.RetryAt != p.RetryAt {
		t.Fatal("restart rerolled schedule")
	}
	raw, _ := os.ReadFile(s.file)
	for _, private := range []string{"synthetic-checkin-token", "synthetic-checkin-user", target.account.Path} {
		if strings.Contains(string(raw), private) {
			t.Fatalf("state contains private identity: %s", private)
		}
	}
}

func TestCheckinSuccessAndAlreadyDonePreventAllFurtherAttempts(t *testing.T) {
	for _, outcome := range []string{"ok", "already"} {
		t.Run(outcome, func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			calls := 0
			s.perform = func(context.Context, *Account, string) (string, error) { calls++; return outcome, nil }
			s.planAndClaim(*now, targets, true)
			*now = checkinAt(8, 45)
			jobs := s.planAndClaim(*now, targets, false)
			if len(jobs) != 1 || jobs[0].attempt != 1 {
				t.Fatal("first scheduled attempt missing")
			}
			var saved checkinDocument
			raw, _ := os.ReadFile(s.file)
			json.Unmarshal(raw, &saved)
			if saved.Plans[target.key].Attempts != 1 || saved.Plans[target.key].Status != "running" {
				t.Fatal("request was dispatched before attempt reservation persisted")
			}
			if len(s.planAndClaim(*now, targets, false)) != 0 {
				t.Fatal("inflight attempt duplicated")
			}
			s.runJob(context.Background(), jobs[0])
			*now = checkinAt(9, 45)
			if len(s.planAndClaim(*now, targets, false)) != 0 || calls != 1 {
				t.Fatal("success retried")
			}
			if !s.nextLocked(*now).IsZero() {
				t.Fatal("finished day must not advertise a fabricated fixed next-day sign-in time")
			}
			if view := s.view(target, *now); view.Status != "success" || checkinDisplay(view, "cn", *now) != "已签" {
				t.Fatalf("success display: %+v", view)
			}
			restarted := newDailyCheckinScheduler(s.file)
			restarted.planAndClaim(*now, targets, true)
			if restarted.view(target, *now).Status != "success" || len(restarted.planAndClaim(*now, targets, false)) != 0 {
				t.Fatal("success not retained across restart")
			}
		})
	}
}

func TestCheckinFailureRetriesExactlyOnceInSecondWindow(t *testing.T) {
	for _, retryOK := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-fails", true: "retry-succeeds"}[retryOK], func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			calls := 0
			s.perform = func(context.Context, *Account, string) (string, error) {
				calls++
				if retryOK && calls == 2 {
					return "ok", nil
				}
				return "failed", errors.New("synthetic-private-token-must-not-be-persisted")
			}
			s.planAndClaim(*now, targets, true)
			*now = checkinAt(8, 45)
			s.runJob(context.Background(), s.planAndClaim(*now, targets, false)[0])
			if view := s.view(target, *now); view.Status != "failed" || view.Attempts != 1 || view.NextAt != checkinAt(9, 45).Unix() {
				t.Fatalf("failure/retry state wrong: %+v", view)
			}
			for _, at := range []time.Time{checkinAt(9, 0), checkinAt(9, 30), checkinAt(9, 44)} {
				if len(s.planAndClaim(at, targets, false)) != 0 {
					t.Fatal("retry before random retry time")
				}
			}
			*now = checkinAt(9, 45)
			jobs := s.planAndClaim(*now, targets, false)
			if len(jobs) != 1 || jobs[0].attempt != 2 {
				t.Fatal("exactly one retry required")
			}
			s.runJob(context.Background(), jobs[0])
			for _, at := range []time.Time{checkinAt(9, 59), checkinAt(10, 0), checkinAt(23, 59)} {
				if len(s.planAndClaim(at, targets, false)) != 0 {
					t.Fatal("third or out-of-window attempt")
				}
			}
			if calls != 2 {
				t.Fatalf("calls=%d", calls)
			}
			raw, _ := os.ReadFile(s.file)
			if strings.Contains(string(raw), "synthetic-private-token") {
				t.Fatal("upstream error leaked into persisted state")
			}
		})
	}
}

func TestCheckinRestartNeverCatchesUpMissedFirstOrRetry(t *testing.T) {
	for _, mode := range []string{"first", "retry", "interrupted-first", "interrupted-retry"} {
		t.Run(mode, func(t *testing.T) {
			s, now, target := checkinFixture(t)
			targets := []checkinTarget{target}
			s.planAndClaim(*now, targets, true)
			p := s.doc.Plans[target.key]
			restartAt := checkinAt(8, 50)
			switch mode {
			case "retry":
				p.Attempts, p.Status = 1, "failed"
				restartAt = checkinAt(9, 50)
			case "interrupted-first":
				p.Attempts, p.Status = 1, "running"
				restartAt = checkinAt(9, 0)
			case "interrupted-retry":
				p.Attempts, p.Status = 2, "running"
				restartAt = checkinAt(9, 50)
			}
			s.saveLocked("synthetic-fixture")
			n := newDailyCheckinScheduler(s.file)
			if len(n.planAndClaim(restartAt, targets, true)) != 0 || len(n.planAndClaim(restartAt, targets, false)) != 0 {
				t.Fatal("restart caught up a past request")
			}
			if mode == "interrupted-first" {
				jobs := n.planAndClaim(checkinAt(9, 45), targets, false)
				if len(jobs) != 1 || jobs[0].attempt != 2 {
					t.Fatal("uncertain first attempt may only use the one scheduled retry")
				}
			} else if len(n.planAndClaim(checkinAt(9, 59), targets, false)) != 0 {
				t.Fatal("missed or exhausted attempt reappeared")
			}
		})
	}
}

func TestCheckinNewAccountsDoNotTriggerEarlyOrCatchupRequests(t *testing.T) {
	s, now, target := checkinFixture(t)
	s.planAndClaim(*now, nil, true)
	if jobs := s.planAndClaim(checkinAt(8, 50), []checkinTarget{target}, false); len(jobs) != 0 {
		t.Fatal("late account addition triggered catchup")
	}
	if s.view(target, checkinAt(8, 50)).Status != "missed" {
		t.Fatal("past plan should be marked missed")
	}
}

func TestCheckinUTC8DayRollAndClockRollback(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	s.doc.Plans[target.key].Status, s.doc.Plans[target.key].Attempts = "success", 1
	s.saveLocked("synthetic-fixture")
	yesterday := s.view(target, *now)
	tomorrow := time.Date(2026, 10, 25, 16, 1, 0, 0, time.UTC) // UTC+8 October 26
	if got := checkinDisplay(yesterday, "cn", tomorrow); got != "待签" {
		t.Fatalf("yesterday success leaked into today: %s", got)
	}
	if len(s.planAndClaim(tomorrow, targets, false)) != 0 || s.doc.Date != "2026-10-26" {
		t.Fatal("midnight triggered a checkin or failed to roll the Beijing day")
	}
	if view := s.view(target, tomorrow); view.Status != "pending" || view.Attempts != 0 {
		t.Fatalf("new day state: %+v", view)
	}
	if len(s.planAndClaim(checkinAt(8, 45), targets, false)) != 0 || !s.blocked {
		t.Fatal("clock rollback must fail closed instead of repeating a past day")
	}
}

func TestCheckinStorageFailuresFailClosedAndKeepOriginal(t *testing.T) {
	s, now, target := checkinFixture(t)
	const broken = `{"schema":1,"secret":"synthetic-private-value"`
	if err := os.WriteFile(s.file, []byte(broken), 0600); err != nil {
		t.Fatal(err)
	}
	if len(s.planAndClaim(*now, []checkinTarget{target}, true)) != 0 || !s.blocked {
		t.Fatal("corrupt state must disable dispatch")
	}
	raw, _ := os.ReadFile(s.file)
	if string(raw) != broken {
		t.Fatal("corrupt state was overwritten")
	}
	if s.view(target, *now).Status != "unknown" {
		t.Fatal("storage failure must not be shown as success")
	}
}

func TestCheckinReservationWriteFailurePreventsDispatch(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	if err := os.Remove(s.file); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(s.file, 0700); err != nil {
		t.Fatal(err)
	}
	if len(s.planAndClaim(checkinAt(8, 45), targets, false)) != 0 || !s.blocked {
		t.Fatal("must never dispatch without durably reserving the attempt")
	}
}

func TestCheckinConcurrentTicksAndDuplicateCredentialsReserveOnce(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target, target}
	s.planAndClaim(*now, targets, true)
	var wg sync.WaitGroup
	out := make(chan []checkinJob, 24)
	for i := 0; i < cap(out); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out <- s.planAndClaim(checkinAt(8, 45), targets, false)
		}()
	}
	wg.Wait()
	close(out)
	count := 0
	for jobs := range out {
		count += len(jobs)
	}
	if count != 1 {
		t.Fatalf("same account duplicated: %d", count)
	}
}

func TestCheckinRandomTimesStayInWindowsAndArePerAccount(t *testing.T) {
	s, now, target := checkinFixture(t)
	s.random = randomCheckinOffset
	targets := make([]checkinTarget, 32)
	for i := range targets {
		targets[i] = target
		targets[i].key = strings.Repeat("0", 62) + string("0123456789abcdef"[i/16]) + string("0123456789abcdef"[i%16])
	}
	s.planAndClaim(*now, targets, true)
	start, retry, end := checkinWindows(*now)
	distinct := map[int64]bool{}
	for _, p := range s.doc.Plans {
		if p.FirstAt < start.UnixMilli() || p.FirstAt >= retry.UnixMilli() || p.RetryAt < retry.UnixMilli() || p.RetryAt >= end.UnixMilli() {
			t.Fatal("random target outside the allowed windows")
		}
		distinct[p.FirstAt] = true
	}
	if len(distinct) < 2 {
		t.Fatal("accounts do not have independent schedules")
	}
}

func TestCheckinTransportExplicitSuccessAndOneHTTPRequest(t *testing.T) {
	for _, tc := range []struct {
		name, body, result string
		status             int
	}{
		{"success", `{"code":0}`, "ok", 200},
		{"already", `{"code":10001,"msg":"今天已签到"}`, "already", 409},
		{"missing-code", `{}`, "failed", 200},
		{"http-failure", `{"code":0}`, "failed", 500},
		{"auth-failure", `{"code":14001,"msg":"已签到"}`, "failed", 401},
		{"bad-json", `<html>error</html>`, "failed", 200},
		{"business-failure", `{"code":10002}`, "failed", 200},
		{"unrelated-error-message", `{"code":10002,"msg":"已签到状态检查失败"}`, "failed", 200},
		{"no-redirect-replay", `{"code":0}`, "failed", 307},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Header.Get("Authorization") != "Bearer synthetic-token" || r.Header.Get("X-Trace-ID") != "" {
					t.Error("unexpected change to upstream authentication/fingerprint headers")
				}
				if tc.status == 307 {
					w.Header().Set("Location", "/would-replay")
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
			profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
			defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
			acc := &Account{Auth: &StoredAuth{Auth: StoredTokens{AccessToken: "synthetic-token"}}}
			got, err := checkinAccountWithTrace(context.Background(), acc, "synthetic-checkin-trace")
			if got != tc.result || calls != 1 || (err != nil) != (got == "failed") {
				t.Fatalf("got=%s err=%v calls=%d", got, err, calls)
			}
		})
	}
}

func TestCheckinDoesNotHoldModelLockDuringNetworkIO(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release
		w.Write([]byte(`{"code":0}`))
	}))
	defer server.Close()
	oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
	profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
	defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
	acc := &Account{Auth: &StoredAuth{Auth: StoredTokens{AccessToken: "synthetic-token"}}}
	done := make(chan struct{})
	go func() { checkinAccount(context.Background(), acc); close(done) }()
	<-arrived
	available := acc.lock.TryRLock()
	if available {
		acc.lock.RUnlock()
	}
	close(release)
	<-done
	if !available {
		t.Fatal("checkin network IO blocked model credential reads")
	}
}

func TestCheckinSnapshotAndTableShowTodayWithoutChangingModelLedger(t *testing.T) {
	isolateAdminFeatures(t)
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	s.planAndClaim(*now, targets, true)
	s.doc.Plans[target.key].Status, s.doc.Plans[target.key].Attempts = "success", 1
	s.saveLocked("synthetic-fixture")
	// Status snapshots use the real clock; align only this display fixture.
	s.doc.Date = checkinDay(time.Now())
	if err := writeStatusSnapshot(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(statusSnapshotFile)
	var snapshot statusSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil || len(snapshot.Accounts) != 1 {
		t.Fatalf("snapshot: %v", err)
	}
	view := snapshot.Accounts[0].Checkin
	if view.Status != "success" || view.Date != checkinDay(time.Now()) {
		t.Fatalf("snapshot checkin missing: %+v", view)
	}
	table := renderAccountTable(snapshot.Accounts)
	if !strings.Contains(table, "签到") || !strings.Contains(table, "已签") || strings.Contains(table, "免费模型") {
		t.Fatalf("wrong monitor table: %s", table)
	}
	if target.account.ModelStates != nil {
		t.Fatal("checkin presentation modified model ledger")
	}
	before, _ := os.ReadFile(s.file)
	w := adminRequest(newWebUIHandler(), http.MethodGet, "/admin/api/status", "feature-admin-key", "")
	var response webStatusResponse
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil ||
		len(response.Accounts) != 1 || response.Accounts[0].Checkin.Status != "success" {
		t.Fatalf("authenticated status API lost today's checkin: HTTP %d", w.Code)
	}
	after, _ := os.ReadFile(s.file)
	if string(before) != string(after) {
		t.Fatal("reading status rewrote the schedule")
	}
	row := taskByID(t, taskViewDefinitions(), "daily_checkin")
	if !strings.Contains(row.Schedule, "08:00–09:30") || !strings.Contains(row.Schedule, "09:30–10:00") ||
		!strings.Contains(row.Schedule, "不补签") || strings.Contains(row.Schedule, "事件可能提前触发") {
		t.Fatalf("task page advertises old scheduling semantics: %s", row.Schedule)
	}
}

func TestCheckinCredentialIdentityChangeCannotUseOldReservation(t *testing.T) {
	s, _, target := checkinFixture(t)
	ctx := context.WithValue(context.Background(), checkinExpectedIdentity{}, target.key)
	accountMu.Lock()
	target.account.Auth.Account.UID = "different-synthetic-user"
	accountMu.Unlock()
	// No HTTP client is required: identity mismatch must be rejected before IO.
	result, err := checkinAccountWithTrace(ctx, target.account, "synthetic-identity-change")
	if result != "skipped" || err != nil {
		t.Fatalf("identity change not stopped: %s %v store=%s", result, err, s.file)
	}
}

func TestCheckinRealBackgroundLoopNeverSendsOnRestart(t *testing.T) {
	s, now, target := checkinFixture(t)
	s.planAndClaim(*now, []checkinTarget{target}, true)
	*now = checkinAt(8, 50) // persisted 08:45 plan is in the past
	var calls atomic.Int32
	n := newDailyCheckinScheduler(s.file)
	n.now = func() time.Time { return *now }
	n.perform = func(context.Context, *Account, string) (string, error) {
		calls.Add(1)
		return "ok", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.run(ctx); close(done) }()
	timer := time.NewTimer(1200 * time.Millisecond)
	<-timer.C // include at least one real timer tick, not just helper calls
	cancel()
	<-done
	if calls.Load() != 0 || n.view(target, *now).Status != "missed" {
		t.Fatal("background startup/timer caught up a missed account")
	}
}

func TestCheckinStrictWindowBoundaries(t *testing.T) {
	for _, stage := range []string{"first-start", "first-end", "retry-start", "retry-end"} {
		t.Run(stage, func(t *testing.T) {
			s, now, target := checkinFixture(t)
			s.random = func(time.Duration) (time.Duration, error) { return 0, nil }
			targets := []checkinTarget{target}
			s.planAndClaim(*now, targets, true)
			at, want := checkinAt(8, 0), 1
			if strings.HasPrefix(stage, "retry") {
				p := s.doc.Plans[target.key]
				p.Status, p.Attempts = "failed", 1
				at = checkinAt(9, 30)
			}
			switch stage {
			case "first-end":
				at, want = checkinAt(9, 30), 0
			case "retry-end":
				at, want = checkinAt(10, 0), 0
			}
			if got := s.planAndClaim(at, targets, false); len(got) != want {
				t.Fatalf("boundary %s got jobs=%d want=%d", stage, len(got), want)
			}
		})
	}
}

func TestCheckinEndToEndTwoHTTPAttemptsSurviveRestart(t *testing.T) {
	s, now, target := checkinFixture(t)
	targets := []checkinTarget{target}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/billing/meter/daily-checkin" || r.Method != http.MethodPost {
			t.Error("unexpected upstream operation")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"code":500,"msg":"synthetic temporary failure"}`))
		} else {
			w.Write([]byte(`{"code":0}`))
		}
	}))
	defer server.Close()
	oldOrigin, oldClient := profileCN.Origin, cfg.HttpClient
	profileCN.Origin, cfg.HttpClient = server.URL, server.Client()
	defer func() { profileCN.Origin, cfg.HttpClient = oldOrigin, oldClient }()
	s.planAndClaim(*now, targets, true)
	if calls.Load() != 0 {
		t.Fatal("startup made an HTTP request")
	}
	*now = checkinAt(8, 45)
	s.runJob(context.Background(), s.planAndClaim(*now, targets, false)[0])
	if calls.Load() != 1 || s.view(target, *now).Status != "failed" {
		t.Fatal("first HTTP failure was not recorded")
	}
	*now = checkinAt(9, 0)
	n := newDailyCheckinScheduler(s.file)
	n.now = func() time.Time { return *now }
	dailyCheckins = n
	n.planAndClaim(*now, targets, true)
	if calls.Load() != 1 || len(n.planAndClaim(*now, targets, false)) != 0 {
		t.Fatal("restart retried outside the retry window")
	}
	*now = checkinAt(9, 45)
	jobs := n.planAndClaim(*now, targets, false)
	if len(jobs) != 1 || jobs[0].attempt != 2 {
		t.Fatal("retry slot lost after restart")
	}
	n.runJob(context.Background(), jobs[0])
	if calls.Load() != 2 || n.view(target, *now).Status != "success" {
		t.Fatal("retry HTTP success missing")
	}
	*now = checkinAt(9, 59)
	restarted := newDailyCheckinScheduler(n.file)
	restarted.planAndClaim(*now, targets, true)
	if len(restarted.planAndClaim(*now, targets, false)) != 0 || calls.Load() != 2 {
		t.Fatal("third HTTP request after success/restart")
	}
}
