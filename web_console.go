package main

import (
	"bytes"
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"nexus-trade-bot/config"
	"nexus-trade-bot/exchange"
	"nexus-trade-bot/logger"
	"nexus-trade-bot/tradestats"
	"nexus-trade-bot/utils"
)

type consoleServer struct {
	configPath    string
	authPath      string
	accountsPath  string
	robotsPath    string
	robotsDir     string
	priceBasePath string
	settingsPath  string
	sessions      map[string]bool
	loginFailures map[string]loginFailure
	balanceCache  map[string]accountBalanceCache
	pnlCache      map[string]pnlCacheEntry
	metricCache   map[string]robotMetricCache
	auth          authState
	accounts      map[string]*accountProfile
	robots        map[string]*robotDefinition
	processes     map[string]*robotProcess
	baseConfig    *config.Config
	baseRawYAML   []byte
	settings      consoleSettings
	priceBaseMu   sync.Mutex
	pnlCacheMu    sync.Mutex
	mu            sync.RWMutex
	procInspector processInspector
}

type processInspector func(pid int) bool

type consoleSettings struct {
	Timezone string `json:"timezone"`
}

type authState struct {
	Username           string `json:"username"`
	PasswordSalt       string `json:"password_salt"`
	PasswordHash       string `json:"password_hash"`
	RememberTokenHash  string `json:"remember_token_hash,omitempty"`
	MustChangePassword bool   `json:"must_change_password"`
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginFailure struct {
	Count       int
	FirstAt     time.Time
	LockedUntil time.Time
}

type passwordRequest struct {
	OldPassword     string `json:"old_password"`
	NewPassword     string `json:"new_password"`
	ConfirmPassword string `json:"confirm_password"`
}

type consoleStatus struct {
	Authenticated      bool `json:"authenticated"`
	MustChangePassword bool `json:"must_change_password"`
}

type symbolsResponse struct {
	Symbols []string `json:"symbols"`
}

type accountProfile struct {
	ID        string                `json:"id"`
	Name      string                `json:"name"`
	Exchange  string                `json:"exchange"`
	Config    config.ExchangeConfig `json:"config"`
	CreatedAt time.Time             `json:"created_at"`
	UpdatedAt time.Time             `json:"updated_at"`
}

type accountView struct {
	ID        string                `json:"id"`
	Name      string                `json:"name"`
	Exchange  string                `json:"exchange"`
	Config    config.ExchangeConfig `json:"config"`
	CreatedAt time.Time             `json:"created_at"`
	UpdatedAt time.Time             `json:"updated_at"`
}

type robotDefinition struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	ConfigPath string         `json:"config_path"`
	Config     *config.Config `json:"config"`
	AccountID  string         `json:"account_id"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type robotProcess struct {
	Cmd           *exec.Cmd
	Status        string    `json:"status"`
	PID           int       `json:"pid"`
	StartedAt     time.Time `json:"started_at"`
	StoppedAt     time.Time `json:"stopped_at"`
	LastError     string    `json:"last_error"`
	ExitReason    string    `json:"exit_reason"`
	DesiredStatus string
	LogPath       string `json:"log_path,omitempty"`
	logFile       *os.File
	RestartCount  int
}

type robotPayload struct {
	Name      string         `json:"name"`
	Config    *config.Config `json:"config"`
	AccountID string         `json:"account_id"`
}

type accountPayload struct {
	Name     string                `json:"name"`
	Exchange string                `json:"exchange"`
	Config   config.ExchangeConfig `json:"config"`
}

type robotMetric struct {
	Balance                     float64 `json:"balance"`
	AvailableBalance            float64 `json:"available_balance"`
	MarginBalance               float64 `json:"margin_balance"`
	CurrentPrice                float64 `json:"current_price"`
	PriceChangePct              float64 `json:"price_change_pct"`
	LongPosition                float64 `json:"long_position"`
	ShortPosition               float64 `json:"short_position"`
	NetPosition                 float64 `json:"net_position"`
	UnrealizedPNL               float64 `json:"unrealized_pnl"`
	TodayRealizedPNL            float64 `json:"today_realized_pnl"`
	TotalRealizedPNL            float64 `json:"total_realized_pnl"`
	HasExchangeUnrealizedPNL    bool    `json:"-"`
	HasExchangeTodayRealizedPNL bool    `json:"-"`
	HasExchangeTotalRealizedPNL bool    `json:"-"`
	TodayVolume                 float64 `json:"today_volume"`
	TotalVolume                 float64 `json:"total_volume"`
	TodayFees                   float64 `json:"today_fees"`
	TotalFees                   float64 `json:"total_fees"`
	PositionCount               int     `json:"position_count"`
	OpenOrderCount              int     `json:"open_order_count"`
	QuoteAsset                  string  `json:"quote_asset"`
	PriceError                  string  `json:"price_error,omitempty"`
	Error                       string  `json:"error,omitempty"`
}

type accountBalanceView struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Exchange         string    `json:"exchange"`
	Balance          float64   `json:"balance"`
	AvailableBalance float64   `json:"available_balance"`
	MarginBalance    float64   `json:"margin_balance"`
	QuoteAsset       string    `json:"quote_asset"`
	FetchedAt        time.Time `json:"fetched_at,omitempty"`
	Error            string    `json:"error,omitempty"`
}

type accountBalanceCache struct {
	Value            accountBalanceView
	AccountUpdatedAt time.Time
	ExpiresAt        time.Time
}

type pnlCacheEntry struct {
	Value     exchange.PNLSummary
	ExpiresAt time.Time
}

type robotMetricCache struct {
	Value          robotMetric
	RobotUpdatedAt time.Time
	ConfigPath     string
	ExpiresAt      time.Time
}

type robotView struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Config     *config.Config `json:"config"`
	AccountID  string         `json:"account_id"`
	Account    *accountView   `json:"account,omitempty"`
	Status     string         `json:"status"`
	PID        int            `json:"pid"`
	StartedAt  time.Time      `json:"started_at,omitempty"`
	StoppedAt  time.Time      `json:"stopped_at,omitempty"`
	LastError  string         `json:"last_error,omitempty"`
	ExitReason string         `json:"exit_reason,omitempty"`
	Metric     robotMetric    `json:"metric"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

type dashboardResponse struct {
	Summary  dashboardSummary     `json:"summary"`
	Robots   []robotView          `json:"robots"`
	Accounts []accountBalanceView `json:"accounts"`
}

type recentTradesResponse struct {
	Trades []tradestats.TradeRecord `json:"trades"`
}

type robotLogsResponse struct {
	Logs string `json:"logs"`
	Path string `json:"path,omitempty"`
}

type priceBaselineStore struct {
	Items map[string]priceBaselineRecord `json:"items"`
}

type priceBaselineRecord struct {
	Date      string    `json:"date"`
	Baseline  float64   `json:"baseline"`
	UpdatedAt time.Time `json:"updated_at"`
}

type dashboardSummary struct {
	RobotCount         int     `json:"robot_count"`
	RunningCount       int     `json:"running_count"`
	StoppedCount       int     `json:"stopped_count"`
	TodayRealizedPNL   float64 `json:"today_realized_pnl"`
	TotalRealizedPNL   float64 `json:"total_realized_pnl"`
	TodayVolume        float64 `json:"today_volume"`
	TotalVolume        float64 `json:"total_volume"`
	TodayFees          float64 `json:"today_fees"`
	TotalFees          float64 `json:"total_fees"`
	TotalBalance       float64 `json:"total_balance"`
	TotalAvailable     float64 `json:"total_available"`
	TotalUnrealizedPNL float64 `json:"total_unrealized_pnl"`
}

const (
	secretMask                     = "********"
	maxJSONBodyBytes               = 1 << 20
	loginFailureWindow             = 10 * time.Minute
	loginLockDuration              = 5 * time.Minute
	maxLoginFailures               = 5
	robotStartProbeDelay           = 800 * time.Millisecond
	maxRobotAutoRestarts           = 5
	robotMetricCacheTTL            = 5 * time.Second
	passwordPBKDF2Prefix           = "pbkdf2-sha512"
	passwordPBKDF2Iterations       = 200000
	passwordPBKDF2KeyLength        = 32
	hardcodedRiskInterval          = "1m"
	hardcodedRiskVolumeMultiplier  = 3.0
	hardcodedRiskAverageWindow     = 20
	hardcodedRiskRecoveryThreshold = 3
	hardcodedContractRiskEnabled   = true
	hardcodedMaxMarginUsagePct     = 70.0
	hardcodedMaxUnrealizedLossPct  = 20.0
	hardcodedMaxAccountDrawdownPct = 25.0
	hardcodedLiquidationGuardPct   = 5.0
	hardcodedSystemLogLevel        = "INFO"
	hardcodedSystemCancelOnExit    = true
	hardcodedWSReconnectDelay      = 5
	hardcodedWSWriteWait           = 10
	hardcodedWSPongWait            = 60
	hardcodedWSPingInterval        = 20
	hardcodedListenKeyKeepAlive    = 30
	hardcodedPriceSendInterval     = 50
	hardcodedRateLimitRetryDelay   = 1
	hardcodedOrderRetryDelay       = 500
	hardcodedPricePollInterval     = 500
	hardcodedStatusPrintInterval   = 1
	hardcodedOrderCleanupInterval  = 60
	hardcodedBuyWindowSize         = 10
	hardcodedSellWindowSize        = 10
	hardcodedReconcileInterval     = 60
	hardcodedOrderCleanupThreshold = 50
	hardcodedCleanupBatchSize      = 20
	hardcodedMarginLockSeconds     = 20
	hardcodedPositionSafetyCheck   = 100
)

func ensureConfigFile(configPath string) error {
	if _, err := os.Stat(configPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	examplePath := filepath.Join(filepath.Dir(configPath), "config.example.yaml")
	data, err := os.ReadFile(examplePath)
	if err != nil {
		return fmt.Errorf("配置文件不存在，且读取示例配置失败: %w", err)
	}
	return os.WriteFile(configPath, data, 0600)
}

func runWebConsole(configPath string) error {
	server, err := newConsoleServer(configPath)
	if err != nil {
		return err
	}
	return server.run()
}

func newConsoleServer(configPath string) (*consoleServer, error) {
	baseRawYAML, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	baseCfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	rootDir := filepath.Dir(configPath)
	server := &consoleServer{
		configPath:    configPath,
		authPath:      configPath + ".auth.json",
		accountsPath:  filepath.Join(rootDir, "web_console_accounts.json"),
		robotsPath:    filepath.Join(rootDir, "web_console_robots.json"),
		robotsDir:     filepath.Join(rootDir, "web_console_robots"),
		priceBasePath: filepath.Join(rootDir, "web_console_price_baselines.json"),
		settingsPath:  filepath.Join(rootDir, "web_console_settings.json"),
		sessions:      make(map[string]bool),
		loginFailures: make(map[string]loginFailure),
		balanceCache:  make(map[string]accountBalanceCache),
		pnlCache:      make(map[string]pnlCacheEntry),
		metricCache:   make(map[string]robotMetricCache),
		accounts:      make(map[string]*accountProfile),
		robots:        make(map[string]*robotDefinition),
		processes:     make(map[string]*robotProcess),
		baseConfig:    baseCfg,
		baseRawYAML:   baseRawYAML,
		procInspector: processAlive,
	}
	if err := os.MkdirAll(server.robotsDir, 0700); err != nil {
		return nil, err
	}
	_ = os.Chmod(server.robotsDir, 0700)
	if err := server.loadAuth(); err != nil {
		return nil, err
	}
	if err := server.loadAccounts(); err != nil {
		return nil, err
	}
	if err := server.loadSettings(); err != nil {
		return nil, err
	}
	if err := server.loadRobots(); err != nil {
		return nil, err
	}
	server.recoverRobotProcesses()
	_ = os.Chmod(server.authPath, 0600)
	_ = os.Chmod(server.accountsPath, 0600)
	_ = os.Chmod(server.robotsPath, 0600)
	return server, nil
}

func (s *consoleServer) run() error {
	mux := http.NewServeMux()
	mux.Handle("/logo/", http.StripPrefix("/logo/", http.FileServer(http.Dir(filepath.Join(filepath.Dir(s.configPath), "logo")))))
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/login", s.handleLogin)
	mux.HandleFunc("/api/change-password", s.handleChangePassword)
	mux.HandleFunc("/api/dashboard", s.handleDashboard)
	mux.HandleFunc("/api/base-config", s.handleBaseConfig)
	mux.HandleFunc("/api/settings", s.handleSettings)
	mux.HandleFunc("/api/symbols", s.handleSymbols)
	mux.HandleFunc("/api/accounts", s.handleAccounts)
	mux.HandleFunc("/api/accounts/", s.handleAccountAction)
	mux.HandleFunc("/api/robots", s.handleRobots)
	mux.HandleFunc("/api/robots/", s.handleRobotAction)
	mux.HandleFunc("/api/logout", s.handleLogout)

	addr := consoleListenAddr()
	logger.Info("🌐 Web 控制台已启动: http://%s", addr)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return server.ListenAndServe()
}

func consoleListenAddr() string {
	if addr := strings.TrimSpace(os.Getenv("NEXUS_TRADE_BOT_ADDR")); addr != "" {
		return addr
	}
	if addr := strings.TrimSpace(os.Getenv("WEB_CONSOLE_ADDR")); addr != "" {
		return addr
	}
	return "127.0.0.1:8080"
}

func (s *consoleServer) loadAuth() error {
	data, err := os.ReadFile(s.authPath)
	if errors.Is(err, os.ErrNotExist) {
		auth, err := makeAuthState("admin", "admin", true)
		if err != nil {
			return err
		}
		s.auth = auth
		return s.saveAuth()
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.auth)
}

func (s *consoleServer) loadSettings() error {
	data, err := os.ReadFile(s.settingsPath)
	if errors.Is(err, os.ErrNotExist) {
		s.settings = consoleSettings{Timezone: tradestats.CurrentTradingTimezoneName()}
		_ = tradestats.SetTradingDayTimezone(s.settings.Timezone)
		return s.saveSettings()
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &s.settings); err != nil {
		return err
	}
	if strings.TrimSpace(s.settings.Timezone) == "" {
		s.settings.Timezone = tradestats.CurrentTradingTimezoneName()
	}
	if err := tradestats.SetTradingDayTimezone(s.settings.Timezone); err != nil {
		s.settings.Timezone = tradestats.CurrentTradingTimezoneName()
	}
	return nil
}

func (s *consoleServer) saveSettings() error {
	data, err := json.MarshalIndent(s.settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.settingsPath, data, 0600)
}

func (s *consoleServer) saveAuth() error {
	return s.saveAuthState(s.auth)
}

func (s *consoleServer) saveAuthState(auth authState) error {
	data, err := json.MarshalIndent(auth, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.authPath, data, 0600)
}

func makeAuthState(username, password string, mustChange bool) (authState, error) {
	saltBytes := make([]byte, 24)
	if _, err := rand.Read(saltBytes); err != nil {
		return authState{}, err
	}
	salt := hex.EncodeToString(saltBytes)
	hash, err := hashPassword(salt, password)
	if err != nil {
		return authState{}, err
	}
	return authState{
		Username:           username,
		PasswordSalt:       salt,
		PasswordHash:       hash,
		MustChangePassword: mustChange,
	}, nil
}

func hashLegacyPassword(salt, password string) string {
	sum := sha256.Sum256([]byte(salt + ":" + password))
	return hex.EncodeToString(sum[:])
}

func hashPassword(salt, password string) (string, error) {
	saltBytes, err := hex.DecodeString(salt)
	if err != nil {
		return "", err
	}
	key, err := pbkdf2.Key(sha512.New, password, saltBytes, passwordPBKDF2Iterations, passwordPBKDF2KeyLength)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s$%d$%s", passwordPBKDF2Prefix, passwordPBKDF2Iterations, hex.EncodeToString(key)), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func verifyPasswordState(auth authState, password string) bool {
	expected := auth.PasswordHash
	var actual string
	if strings.HasPrefix(expected, passwordPBKDF2Prefix+"$") {
		hash, err := hashPassword(auth.PasswordSalt, password)
		if err != nil {
			return false
		}
		actual = hash
	} else {
		actual = hashLegacyPassword(auth.PasswordSalt, password)
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}

func (s *consoleServer) authSnapshot() authState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.auth
}

func (s *consoleServer) currentTimezone() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTimezoneLocked()
}

func (s *consoleServer) currentTimezoneLocked() string {
	if strings.TrimSpace(s.settings.Timezone) != "" {
		return s.settings.Timezone
	}
	return tradestats.CurrentTradingTimezoneName()
}

func (s *consoleServer) settingsSnapshot() consoleSettings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.settings
}

func (s *consoleServer) mustChangePassword() bool {
	return s.authSnapshot().MustChangePassword
}

func (s *consoleServer) verifyPassword(password string) bool {
	return verifyPasswordState(s.authSnapshot(), password)
}

func (s *consoleServer) authenticated(r *http.Request) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if cookie, err := r.Cookie("nexus_trade_bot_session"); err == nil && s.sessions[cookie.Value] {
		return true
	}
	if cookie, err := r.Cookie("nexus_trade_bot_remember"); err == nil && s.auth.RememberTokenHash != "" {
		return subtle.ConstantTimeCompare([]byte(s.auth.RememberTokenHash), []byte(hashToken(cookie.Value))) == 1
	}
	return false
}

func (s *consoleServer) requireAuth(w http.ResponseWriter, r *http.Request) bool {
	if s.authenticated(r) {
		return true
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

func (s *consoleServer) requireReady(w http.ResponseWriter, r *http.Request) bool {
	if !s.requireAuth(w, r) {
		return false
	}
	if s.mustChangePassword() {
		http.Error(w, "must change password", http.StatusForbidden)
		return false
	}
	return true
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		http.Error(w, "bad request", http.StatusBadRequest)
		return false
	}
	return true
}

func (s *consoleServer) loginFailureKey(r *http.Request, username string) string {
	host := r.RemoteAddr
	if h, _, err := strings.Cut(host, ":"); err {
		host = h
	}
	return strings.ToLower(strings.TrimSpace(username)) + "|" + host
}

func (s *consoleServer) loginLocked(key string) bool {
	s.mu.RLock()
	failure := s.loginFailures[key]
	s.mu.RUnlock()
	return time.Now().Before(failure.LockedUntil)
}

func (s *consoleServer) recordLoginFailure(key string) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	failure := s.loginFailures[key]
	if now.Sub(failure.FirstAt) > loginFailureWindow {
		failure = loginFailure{FirstAt: now}
	}
	failure.Count++
	if failure.Count >= maxLoginFailures {
		failure.LockedUntil = now.Add(loginLockDuration)
	}
	s.loginFailures[key] = failure
}

func (s *consoleServer) clearLoginFailure(key string) {
	s.mu.Lock()
	delete(s.loginFailures, key)
	s.mu.Unlock()
}

func (s *consoleServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, consoleStatus{Authenticated: s.authenticated(r), MustChangePassword: s.mustChangePassword()})
}

func (s *consoleServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req loginRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	key := s.loginFailureKey(r, req.Username)
	if s.loginLocked(key) {
		http.Error(w, "登录失败次数过多，请稍后再试", http.StatusTooManyRequests)
		return
	}
	auth := s.authSnapshot()
	if req.Username != auth.Username || !verifyPasswordState(auth, req.Password) {
		s.recordLoginFailure(key)
		http.Error(w, "用户名或密码错误", http.StatusUnauthorized)
		return
	}
	s.clearLoginFailure(key)
	if !strings.HasPrefix(auth.PasswordHash, passwordPBKDF2Prefix+"$") {
		if migrated, err := makeAuthState(auth.Username, req.Password, auth.MustChangePassword); err == nil {
			s.mu.Lock()
			if s.auth.PasswordHash == auth.PasswordHash {
				s.auth = migrated
				auth = migrated
			}
			s.mu.Unlock()
			_ = s.saveAuthState(auth)
		}
	}
	sessionBytes := make([]byte, 32)
	if _, err := rand.Read(sessionBytes); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	token := base64.RawURLEncoding.EncodeToString(sessionBytes)
	s.mu.Lock()
	s.sessions[token] = true
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "nexus_trade_bot_session", Value: token, Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode})
	rememberBytes := make([]byte, 32)
	if _, err := rand.Read(rememberBytes); err == nil {
		rememberToken := base64.RawURLEncoding.EncodeToString(rememberBytes)
		s.mu.Lock()
		s.auth.RememberTokenHash = hashToken(rememberToken)
		auth = s.auth
		s.mu.Unlock()
		_ = s.saveAuthState(auth)
		http.SetCookie(w, &http.Cookie{Name: "nexus_trade_bot_remember", Value: rememberToken, Path: "/", HttpOnly: true, Secure: r.TLS != nil, MaxAge: 86400 * 30, SameSite: http.SameSiteStrictMode})
	}
	writeJSON(w, consoleStatus{Authenticated: true, MustChangePassword: auth.MustChangePassword})
}

func (s *consoleServer) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cookie, err := r.Cookie("nexus_trade_bot_session")
	s.mu.Lock()
	if err == nil {
		delete(s.sessions, cookie.Value)
	}
	s.auth.RememberTokenHash = ""
	_ = s.saveAuth()
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: "nexus_trade_bot_session", Value: "", Path: "/", HttpOnly: true, MaxAge: -1, SameSite: http.SameSiteStrictMode})
	http.SetCookie(w, &http.Cookie{Name: "nexus_trade_bot_remember", Value: "", Path: "/", HttpOnly: true, MaxAge: -1, SameSite: http.SameSiteStrictMode})
	writeJSON(w, map[string]bool{"ok": true})
}

func (s *consoleServer) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req passwordRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	authSnapshot := s.authSnapshot()
	if req.NewPassword != req.ConfirmPassword {
		http.Error(w, "两次输入的新密码不一致", http.StatusBadRequest)
		return
	}
	if !authSnapshot.MustChangePassword && !verifyPasswordState(authSnapshot, req.OldPassword) {
		http.Error(w, "旧密码错误", http.StatusUnauthorized)
		return
	}
	if !strongEnoughPassword(req.NewPassword) {
		http.Error(w, "新密码至少 12 位，并且不能为 admin 或纯空白", http.StatusBadRequest)
		return
	}
	auth, err := makeAuthState(authSnapshot.Username, req.NewPassword, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.mu.Lock()
	s.auth = auth
	s.mu.Unlock()
	if err := s.saveAuthState(auth); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, consoleStatus{Authenticated: true, MustChangePassword: false})
}

func (s *consoleServer) handleBaseConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	resp := map[string]interface{}{
		"config":   publicConfig(s.baseConfig),
		"timezone": s.currentTimezone(),
	}
	writeJSON(w, resp)
}

func (s *consoleServer) handleSettings(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.settingsSnapshot())
	case http.MethodPut:
		var payload consoleSettings
		if !decodeJSONBody(w, r, &payload) {
			return
		}
		payload.Timezone = strings.TrimSpace(payload.Timezone)
		if payload.Timezone == "" {
			payload.Timezone = tradestats.CurrentTradingTimezoneName()
		}
		if _, err := time.LoadLocation(payload.Timezone); err != nil {
			http.Error(w, "invalid timezone", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.settings.Timezone = payload.Timezone
		_ = tradestats.SetTradingDayTimezone(payload.Timezone)
		err := s.saveSettings()
		s.mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, s.settingsSnapshot())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *consoleServer) handleSymbols(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	marketType := normalizeMarketTypeParam(r.URL.Query().Get("market_type"))
	symbols := collectPublicSymbols(marketType)
	writeJSON(w, symbolsResponse{Symbols: symbols})
}

func (s *consoleServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	views := s.collectRobotViews()
	activeConfigPaths := s.collectRobotConfigPaths()
	accountBalances := s.collectAccountBalances()
	var summary dashboardSummary
	summary.RobotCount = len(views)
	for _, robot := range views {
		if robot.Status == "running" {
			summary.RunningCount++
		} else {
			summary.StoppedCount++
		}
		summary.TodayRealizedPNL += robot.Metric.TodayRealizedPNL
		summary.TotalRealizedPNL += robot.Metric.TotalRealizedPNL
		summary.TodayVolume += robot.Metric.TodayVolume
		summary.TotalVolume += robot.Metric.TotalVolume
		summary.TodayFees += robot.Metric.TodayFees
		summary.TotalFees += robot.Metric.TotalFees
		summary.TotalUnrealizedPNL += robot.Metric.UnrealizedPNL
	}
	for _, metric := range s.collectArchivedRobotMetrics(activeConfigPaths) {
		summary.TodayRealizedPNL += metric.TodayRealizedPNL
		summary.TotalRealizedPNL += metric.TotalRealizedPNL
		summary.TodayVolume += metric.TodayVolume
		summary.TotalVolume += metric.TotalVolume
		summary.TodayFees += metric.TodayFees
		summary.TotalFees += metric.TotalFees
	}
	for _, account := range accountBalances {
		if account.Error != "" {
			continue
		}
		summary.TotalBalance += account.Balance
		summary.TotalAvailable += account.AvailableBalance
	}
	writeJSON(w, dashboardResponse{Summary: summary, Robots: views, Accounts: accountBalances})
}

func (s *consoleServer) handleAccounts(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.collectAccountViews())
	case http.MethodPost:
		var payload accountPayload
		if !decodeJSONBody(w, r, &payload) {
			return
		}
		account, err := s.createAccount(payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, publicAccountView(account))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *consoleServer) handleAccountAction(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/accounts/"), "/")
	if id == "" {
		http.Error(w, "account id required", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		var payload accountPayload
		if !decodeJSONBody(w, r, &payload) {
			return
		}
		account, err := s.updateAccount(id, payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, publicAccountView(account))
	case http.MethodDelete:
		if err := s.deleteAccount(id); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]bool{"deleted": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *consoleServer) handleRobots(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.collectRobotViews())
	case http.MethodPost:
		var payload robotPayload
		if !decodeJSONBody(w, r, &payload) {
			return
		}
		robot, err := s.createRobot(payload)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, robot)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *consoleServer) handleRobotAction(w http.ResponseWriter, r *http.Request) {
	if !s.requireReady(w, r) {
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/robots/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "robot id required", http.StatusBadRequest)
		return
	}
	id := parts[0]
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPut:
			var payload robotPayload
			if !decodeJSONBody(w, r, &payload) {
				return
			}
			robot, err := s.updateRobot(id, payload)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, robot)
		case http.MethodDelete:
			if err := s.deleteRobot(id); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]bool{"deleted": true})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	action := parts[1]
	if action == "trades" {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		trades, err := s.recentRobotTrades(id, 100)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, recentTradesResponse{Trades: trades})
		return
	}
	if action == "logs" {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		logs, path, err := s.robotLogs(id, 256*1024)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, robotLogsResponse{Logs: logs, Path: path})
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch action {
	case "start":
		if err := s.startRobot(id); err != nil {
			http.Error(w, friendlyErrorMessage(err), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]bool{"started": true})
	case "pause":
		if err := s.pauseRobot(id); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]bool{"paused": true})
	case "stop":
		if err := s.stopRobot(id); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]bool{"stopped": true})
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
	}
}

func (s *consoleServer) recentRobotTrades(id string, limit int) ([]tradestats.TradeRecord, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	s.mu.RLock()
	robot, ok := s.robots[id]
	if !ok || robot == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("机器人不存在")
	}
	configPath := robot.ConfigPath
	symbol := robot.Config.Trading.Symbol
	cfg := cloneConfig(robot.Config)
	s.mu.RUnlock()

	snap, err := tradestats.LoadWithLogFallback(tradestats.PathForConfig(configPath), robotLogPath(configPath), 0)
	if err != nil {
		return nil, fmt.Errorf("读取近期成交失败: %w", err)
	}
	trades := tradestats.FilterTradesBySymbol(snap.RecentTrades, symbol)
	if len(trades) > limit {
		trades = trades[len(trades)-limit:]
	}
	if currentPosition, ok := liveNetPosition(cfg, symbol, 5*time.Second); ok {
		after := currentPosition
		for i := len(trades) - 1; i >= 0; i-- {
			trades[i].PositionAfter = after
			after -= trades[i].PositionDelta
		}
	}
	out := make([]tradestats.TradeRecord, 0, len(trades))
	for i := len(trades) - 1; i >= 0; i-- {
		out = append(out, trades[i])
	}
	return out, nil
}

func liveNetPosition(cfg *config.Config, symbol string, timeout time.Duration) (float64, bool) {
	ex, err := exchange.NewExchange(cfg)
	if err != nil {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	positions, err := ex.GetPositions(ctx, symbol)
	if err != nil {
		return 0, false
	}
	var net float64
	for _, pos := range positions {
		if pos != nil {
			net += pos.Size
		}
	}
	return net, true
}

func (s *consoleServer) robotLogs(id string, maxBytes int) (string, string, error) {
	if maxBytes <= 0 {
		maxBytes = 256 * 1024
	}
	s.mu.RLock()
	robot, ok := s.robots[id]
	if !ok || robot == nil {
		s.mu.RUnlock()
		return "", "", fmt.Errorf("机器人不存在")
	}
	configPath := robot.ConfigPath
	s.mu.RUnlock()

	logPath := robotLogPath(configPath)
	if !strings.HasPrefix(filepath.Clean(logPath), filepath.Clean(s.robotsDir)+string(os.PathSeparator)) {
		return "", "", fmt.Errorf("机器人日志路径无效")
	}
	logs, err := tailTextFileBytes(logPath, maxBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", logPath, nil
		}
		return "", "", fmt.Errorf("读取机器人日志失败: %w", err)
	}
	return logs, logPath, nil
}

func (s *consoleServer) createRobot(payload robotPayload) (*robotView, error) {
	cfg, err := s.normalizeRobotConfig(payload.Config)
	if err != nil {
		return nil, err
	}
	account, err := s.applyAccountToRobotConfig(payload.AccountID, cfg)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	id := makeRobotID(autoRobotName(cfg))
	cfg.Trading.OrderTag = robotOrderTag(id)
	now := time.Now()
	autoName := autoRobotName(cfg)
	robot := &robotDefinition{
		ID:         id,
		Name:       autoName,
		ConfigPath: filepath.Join(s.robotsDir, id+".yaml"),
		Config:     cfg,
		AccountID:  account.ID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	account, ok := s.accounts[payload.AccountID]
	if !ok || account == nil {
		return nil, fmt.Errorf("必须先选择一个已保存账户")
	}
	if err := applyAccountConfigToConfig(cfg, account); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if _, exists := s.robots[id]; exists {
		return nil, fmt.Errorf("机器人 ID 冲突，请重试")
	}
	if err := s.ensureNoConflictingRobotLocked(id, account.ID, cfg); err != nil {
		return nil, err
	}
	if err := config.SaveConfig(robot.ConfigPath, robot.Config); err != nil {
		return nil, err
	}
	s.robots[id] = robot
	if err := s.saveRobotsLocked(); err != nil {
		delete(s.robots, id)
		return nil, err
	}
	view := s.buildRobotViewLocked(robot)
	return &view, nil
}

func (s *consoleServer) updateRobot(id string, payload robotPayload) (*robotView, error) {
	cfg, err := s.normalizeRobotConfig(payload.Config)
	if err != nil {
		return nil, err
	}
	account, err := s.applyAccountToRobotConfig(payload.AccountID, cfg)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	robot, ok := s.robots[id]
	if !ok {
		return nil, fmt.Errorf("机器人不存在")
	}
	if proc, exists := s.processes[id]; exists && proc.Status == "running" {
		return nil, fmt.Errorf("编辑前必须先暂停机器人")
	}
	account, ok = s.accounts[payload.AccountID]
	if !ok || account == nil {
		return nil, fmt.Errorf("必须先选择一个已保存账户")
	}
	if err := applyAccountConfigToConfig(cfg, account); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := s.ensureNoConflictingRobotLocked(id, account.ID, cfg); err != nil {
		return nil, err
	}
	cfg.Trading.OrderTag = robotOrderTag(id)
	oldConfigBytes, _ := json.Marshal(robot.Config)
	newConfigBytes, _ := json.Marshal(cfg)
	oldAccountID := robot.AccountID
	newName := autoRobotName(cfg)
	shouldRestart := string(oldConfigBytes) != string(newConfigBytes) || robot.Name != newName || oldAccountID != account.ID
	robot.Name = newName
	robot.Config = cfg
	robot.AccountID = account.ID
	robot.UpdatedAt = time.Now()
	if err := config.SaveConfig(robot.ConfigPath, robot.Config); err != nil {
		return nil, err
	}
	if err := s.saveRobotsLocked(); err != nil {
		return nil, err
	}
	if proc, exists := s.processes[id]; exists && proc.Status == "paused" && shouldRestart {
		proc.Status = "stopped"
		proc.DesiredStatus = "stopped"
	}
	view := s.buildRobotViewLocked(robot)
	return &view, nil
}

func (s *consoleServer) deleteRobot(id string) error {
	if err := s.stopRobot(id); err != nil && !strings.Contains(err.Error(), "未运行") {
		return err
	}
	if err := s.cancelRobotOpenOrders(id); err != nil {
		if isIgnorableCancelErrorForDelete(err) {
			logger.Warn("⚠️ [删除机器人] 清空交易对挂单失败（将继续删除）: robot=%s err=%v", id, err)
		} else {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	robot, ok := s.robots[id]
	if !ok {
		return fmt.Errorf("机器人不存在")
	}
	delete(s.robots, id)
	delete(s.processes, id)
	_ = os.Remove(robot.ConfigPath)
	return s.saveRobotsLocked()
}

func isIgnorableCancelErrorForDelete(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "-2015"),
		strings.Contains(lower, "invalid api-key"),
		strings.Contains(lower, "invalid api key"),
		strings.Contains(lower, "invalid key"),
		strings.Contains(lower, "api key"),
		strings.Contains(lower, "apikey"),
		strings.Contains(lower, "unauthorized"),
		strings.Contains(lower, "forbidden"),
		strings.Contains(lower, "permission"),
		strings.Contains(lower, "not permitted"),
		strings.Contains(lower, "not allowed"),
		strings.Contains(lower, "status=401"):
		return true
	default:
		return false
	}
}

func (s *consoleServer) startRobot(id string) error {
	s.mu.Lock()
	delete(s.metricCache, id)
	robot, ok := s.robots[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("机器人不存在")
	}
	if proc, exists := s.processes[id]; exists && proc.Status == "running" {
		s.mu.Unlock()
		return fmt.Errorf("机器人已在运行")
	}
	if err := s.ensureNoConflictingRobotLocked(id, robot.AccountID, robot.Config); err != nil {
		s.mu.Unlock()
		return err
	}
	if pid, ok := s.runningPIDFromFile(id); ok {
		s.processes[id] = &robotProcess{
			Status:        "running",
			PID:           pid,
			StartedAt:     time.Now(),
			DesiredStatus: "running",
			LogPath:       filepath.Join(s.robotsDir, id+".log"),
			ExitReason:    "已接管现有 worker 进程",
		}
		s.mu.Unlock()
		go s.monitorAdoptedRobot(id, pid)
		return nil
	}
	if pids := findWorkerPIDsForConfig(robot.ConfigPath); len(pids) > 0 {
		pid := pids[0]
		s.processes[id] = &robotProcess{
			Status:        "running",
			PID:           pid,
			StartedAt:     time.Now(),
			DesiredStatus: "running",
			LogPath:       filepath.Join(s.robotsDir, id+".log"),
			ExitReason:    "已按配置路径接管遗留 worker 进程",
		}
		_ = s.writeRobotPID(id, pid)
		if len(pids) > 1 {
			logger.Warn("⚠️ [进程接管] %s 发现 %d 个重复 worker，将保留首个并停止其余进程: %v", id, len(pids), pids[1:])
			go stopExtraWorkerPIDs(pids[1:])
		}
		s.mu.Unlock()
		go s.monitorAdoptedRobot(id, pid)
		return nil
	}
	account, ok := s.accounts[robot.AccountID]
	if !ok || account == nil {
		s.mu.Unlock()
		return fmt.Errorf("机器人绑定的账户不存在")
	}
	if err := applyAccountConfigToRobot(robot, account); err != nil {
		s.mu.Unlock()
		return err
	}
	ensureRobotOrderTag(robot)
	if err := config.SaveConfig(robot.ConfigPath, robot.Config); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("保存机器人最新账户配置失败: %w", err)
	}
	executable, err := os.Executable()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	logPath := filepath.Join(s.robotsDir, id+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("无法创建机器人日志文件: %w", err)
	}
	_, _ = fmt.Fprintf(logFile, "\n===== robot start %s =====\nid=%s name=%s exchange=%s market=%s symbol=%s direction=%s config=%s\n",
		time.Now().Format(time.RFC3339), robot.ID, robot.Name, robot.Config.App.CurrentExchange, robot.Config.App.MarketType, robot.Config.Trading.Symbol, robot.Config.Trading.Direction, robot.ConfigPath)
	cmd := exec.Command(executable, "worker", robot.ConfigPath)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), "NEXUS_TRADE_BOT_TIMEZONE="+s.currentTimezoneLocked())
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		s.mu.Unlock()
		return err
	}
	if err := s.writeRobotPID(id, cmd.Process.Pid); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = logFile.Close()
		s.mu.Unlock()
		return fmt.Errorf("写入机器人 PID 文件失败: %w", err)
	}
	restartCount := 0
	if existing := s.processes[id]; existing != nil {
		if existing.Status == "auto-restarting" {
			restartCount = existing.RestartCount
		}
	}
	proc := &robotProcess{Cmd: cmd, Status: "running", PID: cmd.Process.Pid, StartedAt: time.Now(), DesiredStatus: "running", LogPath: logPath, logFile: logFile, RestartCount: restartCount}
	s.processes[id] = proc
	s.mu.Unlock()
	go s.probeRobotStart(id, cmd)
	go s.waitRobot(id, cmd)
	return nil
}

func (s *consoleServer) probeRobotStart(id string, cmd *exec.Cmd) {
	time.Sleep(robotStartProbeDelay)
	s.mu.RLock()
	proc, ok := s.processes[id]
	stillSameProcess := ok && proc.Cmd == cmd && proc.Status == "running"
	s.mu.RUnlock()
	if stillSameProcess {
		return
	}
	s.mu.RLock()
	lastError := ""
	if proc != nil {
		lastError = proc.LastError
	}
	s.mu.RUnlock()
	if lastError != "" {
		logger.Warn("机器人启动失败: %s", lastError)
	}
}

func (s *consoleServer) pauseRobot(id string) error {
	s.mu.Lock()
	if proc, ok := s.processes[id]; ok && proc != nil && proc.Status != "running" && proc.DesiredStatus == "running" {
		pidAlive := proc.PID > 0 && s.isProcessAlive(proc.PID)
		if pid, ok := s.runningPIDFromFile(id); ok {
			proc.PID = pid
			pidAlive = true
		}
		if !pidAlive {
			proc.DesiredStatus = "paused"
			proc.Status = "paused"
			proc.ExitReason = "已暂停自动重启"
			s.mu.Unlock()
			return s.cancelRobotOpenOrders(id)
		}
	}
	s.mu.Unlock()
	if err := s.stopRobotWithStatus(id, "paused"); err != nil {
		return err
	}
	return s.cancelRobotOpenOrders(id)
}

func (s *consoleServer) cancelRobotOpenOrders(id string) error {
	robotCfg, symbol, _, err := s.robotConfigForOrderCancel(id)
	if err != nil {
		return err
	}
	ex, err := exchange.NewExchange(robotCfg)
	if err != nil {
		return fmt.Errorf("创建撤单交易所实例失败: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cancelSymbolOpenOrdersUntilClear(ctx, ex, symbol); err != nil {
		return fmt.Errorf("清空交易对挂单失败: %w", err)
	}
	logger.Info("✅ [机器人撤单] %s 已确认 %s 挂单为 0", id, symbol)
	return nil
}

func (s *consoleServer) robotConfigForOrderCancel(id string) (*config.Config, string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	robot, ok := s.robots[id]
	if !ok || robot == nil || robot.Config == nil {
		return nil, "", "", fmt.Errorf("机器人不存在")
	}
	account, ok := s.accounts[robot.AccountID]
	if !ok || account == nil {
		return nil, "", "", fmt.Errorf("机器人绑定的账户不存在")
	}
	ensureRobotOrderTag(robot)
	if err := applyAccountConfigToRobot(robot, account); err != nil {
		return nil, "", "", err
	}
	cfgCopy := cloneConfig(robot.Config)
	return cfgCopy, robot.Config.Trading.Symbol, robot.Config.Trading.OrderTag, nil
}

func (s *consoleServer) waitRobot(id string, cmd *exec.Cmd) {
	err := cmd.Wait()
	var restartAfter time.Duration
	shouldRestart := false
	s.mu.Lock()
	delete(s.metricCache, id)
	proc, ok := s.processes[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	if proc.DesiredStatus == "paused" {
		proc.Status = "paused"
	} else {
		proc.Status = "stopped"
	}
	proc.StoppedAt = time.Now()
	if proc.logFile != nil {
		_ = proc.logFile.Close()
		proc.logFile = nil
	}
	s.removeRobotPIDIfCurrent(id, proc.PID)
	if err != nil {
		if proc.DesiredStatus == "running" {
			proc.LastError = friendlyRobotExitError(err, proc.LogPath)
			proc.ExitReason = "进程异常退出"
		} else {
			proc.LastError = ""
			if proc.DesiredStatus == "paused" {
				proc.ExitReason = "已暂停"
			} else {
				proc.ExitReason = "已停止"
			}
		}
		if proc.DesiredStatus == "running" && proc.RestartCount < maxRobotAutoRestarts {
			proc.RestartCount++
			restartAfter = robotRestartBackoff(proc.RestartCount)
			proc.ExitReason = fmt.Sprintf("进程异常退出，%s 后自动重启 (%d/%d)", restartAfter, proc.RestartCount, maxRobotAutoRestarts)
			shouldRestart = true
		}
	} else {
		proc.ExitReason = "进程正常退出"
		proc.RestartCount = 0
	}
	s.mu.Unlock()
	if shouldRestart {
		go s.restartRobotAfter(id, cmd, restartAfter)
	}
}

func robotRestartBackoff(restartCount int) time.Duration {
	if restartCount < 1 {
		restartCount = 1
	}
	exp := restartCount - 1
	if exp > 5 {
		exp = 5
	}
	delay := time.Duration(1<<uint(exp)) * 5 * time.Second
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

func (s *consoleServer) restartRobotAfter(id string, cmd *exec.Cmd, delay time.Duration) {
	time.Sleep(delay)
	s.mu.RLock()
	proc, ok := s.processes[id]
	shouldRestart := ok && proc.Cmd == cmd && proc.DesiredStatus == "running" && proc.Status == "stopped"
	s.mu.RUnlock()
	if !shouldRestart {
		return
	}
	s.mu.Lock()
	if proc, ok := s.processes[id]; ok && proc.Cmd == cmd && proc.DesiredStatus == "running" && proc.Status == "stopped" {
		proc.Status = "auto-restarting"
	}
	s.mu.Unlock()
	if err := s.startRobot(id); err != nil {
		logger.Error("机器人自动重启失败: %s: %v", id, err)
	}
}

func (s *consoleServer) stopRobot(id string) error {
	return s.stopRobotWithStatus(id, "stopped")
}

func (s *consoleServer) stopRobotWithStatus(id, finalStatus string) error {
	s.mu.Lock()
	delete(s.metricCache, id)
	proc, ok := s.processes[id]
	var extraPIDs []int
	if !ok || proc == nil {
		if pid, alive := s.runningPIDFromFile(id); alive {
			proc = &robotProcess{
				Status:        "running",
				PID:           pid,
				StartedAt:     time.Now(),
				DesiredStatus: finalStatus,
				LogPath:       filepath.Join(s.robotsDir, id+".log"),
				ExitReason:    "正在停止遗留 worker 进程",
			}
			s.processes[id] = proc
		} else if robot, exists := s.robots[id]; exists && robot != nil {
			pids := findWorkerPIDsForConfig(robot.ConfigPath)
			if len(pids) > 0 {
				pid := pids[0]
				extraPIDs = append(extraPIDs, pids[1:]...)
				proc = &robotProcess{
					Status:        "running",
					PID:           pid,
					StartedAt:     time.Now(),
					DesiredStatus: finalStatus,
					LogPath:       filepath.Join(s.robotsDir, id+".log"),
					ExitReason:    "正在停止按配置路径发现的遗留 worker 进程",
				}
				s.processes[id] = proc
				_ = s.writeRobotPID(id, pid)
			} else {
				s.mu.Unlock()
				return fmt.Errorf("机器人未运行")
			}
		} else {
			s.mu.Unlock()
			return fmt.Errorf("机器人未运行")
		}
	}
	if robot, exists := s.robots[id]; exists && robot != nil {
		extraPIDs = appendMissingPIDs(extraPIDs, findWorkerPIDsForConfig(robot.ConfigPath), proc.PID)
	}
	proc.DesiredStatus = finalStatus
	if proc.Status != "running" {
		if pid, alive := s.runningPIDFromFile(id); alive {
			proc.Status = "running"
			proc.PID = pid
			proc.ExitReason = "正在停止遗留 worker 进程"
		} else if robot, exists := s.robots[id]; exists && robot != nil {
			pids := findWorkerPIDsForConfig(robot.ConfigPath)
			if len(pids) > 0 {
				pid := pids[0]
				extraPIDs = appendMissingPIDs(extraPIDs, pids[1:], pid)
				proc.Status = "running"
				proc.PID = pid
				proc.ExitReason = "正在停止按配置路径发现的遗留 worker 进程"
				_ = s.writeRobotPID(id, pid)
			} else if proc.PID > 0 && s.isProcessAlive(proc.PID) {
				proc.Status = "running"
			} else {
				proc.Status = finalStatus
				proc.ExitReason = "已停止自动重启"
				proc.StoppedAt = time.Now()
				s.removeRobotPIDIfCurrent(id, proc.PID)
				s.mu.Unlock()
				return nil
			}
		} else if proc.PID > 0 && s.isProcessAlive(proc.PID) {
			proc.Status = "running"
		} else {
			proc.Status = finalStatus
			proc.ExitReason = "已停止自动重启"
			proc.StoppedAt = time.Now()
			s.removeRobotPIDIfCurrent(id, proc.PID)
			s.mu.Unlock()
			return nil
		}
	}
	if proc.Status != "running" || proc.PID <= 0 {
		proc.Status = finalStatus
		proc.ExitReason = "已停止自动重启"
		proc.StoppedAt = time.Now()
		s.removeRobotPIDIfCurrent(id, proc.PID)
		s.mu.Unlock()
		return nil
	}
	pid := proc.PID
	if proc.Cmd != nil && proc.Cmd.Process != nil {
		pid = proc.Cmd.Process.Pid
	}
	if pid <= 0 {
		proc.Status = finalStatus
		proc.ExitReason = "已停止"
		proc.StoppedAt = time.Now()
		s.removeRobotPIDIfCurrent(id, proc.PID)
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	stopExtraWorkerPIDs(extraPIDs)
	if err := signalProcessGroup(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		status := proc.Status
		s.mu.RUnlock()
		if status != "running" || !s.isProcessAlive(pid) {
			s.markRobotStopped(id, pid, finalStatus, "已停止")
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	_ = signalProcessGroup(pid, syscall.SIGKILL)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !s.isProcessAlive(pid) {
			s.markRobotStopped(id, pid, finalStatus, "已强制停止")
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	s.markRobotStopped(id, pid, finalStatus, "已发送强制停止信号")
	return nil
}

func (s *consoleServer) robotPIDPath(id string) string {
	return filepath.Join(s.robotsDir, id+".pid")
}

func (s *consoleServer) writeRobotPID(id string, pid int) error {
	return os.WriteFile(s.robotPIDPath(id), []byte(strconv.Itoa(pid)+"\n"), 0600)
}

func (s *consoleServer) readRobotPID(id string) (int, bool) {
	data, err := os.ReadFile(s.robotPIDPath(id))
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

func (s *consoleServer) runningPIDFromFile(id string) (int, bool) {
	pid, ok := s.readRobotPID(id)
	if !ok {
		return 0, false
	}
	if s.isProcessAlive(pid) && s.pidMatchesRobotWorkerLocked(id, pid) {
		return pid, true
	}
	_ = os.Remove(s.robotPIDPath(id))
	return 0, false
}

func (s *consoleServer) isProcessAlive(pid int) bool {
	if s != nil && s.procInspector != nil {
		return s.procInspector(pid)
	}
	return processAlive(pid)
}

func (s *consoleServer) pidMatchesRobotWorkerLocked(id string, pid int) bool {
	if pid <= 0 {
		return false
	}
	robot := s.robots[id]
	configPath := ""
	if robot != nil {
		configPath = robot.ConfigPath
	}
	if strings.TrimSpace(configPath) == "" {
		return true
	}
	return pidCommandUsesWorkerConfig(pid, configPath)
}

func (s *consoleServer) removeRobotPIDIfCurrent(id string, pid int) {
	current, ok := s.readRobotPID(id)
	if !ok || current == pid || pid <= 0 {
		_ = os.Remove(s.robotPIDPath(id))
	}
}

func (s *consoleServer) recoverRobotProcesses() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, robot := range s.robots {
		pid, ok := s.runningPIDFromFile(id)
		if !ok {
			if robot == nil {
				continue
			}
			pids := findWorkerPIDsForConfig(robot.ConfigPath)
			if len(pids) == 0 {
				continue
			}
			pid = pids[0]
			_ = s.writeRobotPID(id, pid)
			if len(pids) > 1 {
				logger.Warn("⚠️ [进程接管] %s 发现 %d 个重复 worker，将保留首个并停止其余进程: %v", id, len(pids), pids[1:])
				go stopExtraWorkerPIDs(pids[1:])
			}
		}
		s.processes[id] = &robotProcess{
			Status:        "running",
			PID:           pid,
			StartedAt:     time.Now(),
			DesiredStatus: "running",
			LogPath:       filepath.Join(s.robotsDir, id+".log"),
			ExitReason:    "控制台重启后已接管现有 worker 进程",
		}
		go s.monitorAdoptedRobot(id, pid)
	}
}

func (s *consoleServer) monitorAdoptedRobot(id string, pid int) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if s.isProcessAlive(pid) {
			continue
		}
		s.markRobotStopped(id, pid, "stopped", "遗留 worker 进程已退出")
		return
	}
}

func (s *consoleServer) markRobotStopped(id string, pid int, status, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	proc, ok := s.processes[id]
	if !ok || proc == nil || (proc.PID != 0 && pid != 0 && proc.PID != pid) {
		return
	}
	proc.Status = status
	proc.DesiredStatus = status
	proc.StoppedAt = time.Now()
	proc.ExitReason = reason
	proc.LastError = ""
	if proc.logFile != nil {
		_ = proc.logFile.Close()
		proc.logFile = nil
	}
	s.removeRobotPIDIfCurrent(id, pid)
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return syscall.ESRCH
	}
	if err := syscall.Kill(-pid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return syscall.Kill(pid, sig)
		}
		return err
	}
	return nil
}

func findWorkerPIDsForConfig(configPath string) []int {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return nil
	}
	targetAbsPath, err := filepath.Abs(configPath)
	if err == nil {
		configPath = targetAbsPath
		if resolvedPath, err := filepath.EvalSymlinks(targetAbsPath); err == nil {
			configPath = resolvedPath
		}
	}
	out, err := exec.Command("ps", "ax", "-o", "pid=,command=").Output()
	if err != nil {
		return nil
	}
	var pids []int
	lines := strings.Split(string(out), "\n")
	currentPID := os.Getpid()
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 || pid == currentPID {
			continue
		}
		if !workerCommandUsesConfig(fields[1:], configPath) {
			continue
		}
		if processAlive(pid) {
			pids = append(pids, pid)
		}
	}
	sort.Ints(pids)
	return pids
}

func pidCommandUsesWorkerConfig(pid int, configPath string) bool {
	if pid <= 0 || strings.TrimSpace(configPath) == "" {
		return false
	}
	out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "command=").Output()
	if err != nil {
		return false
	}
	fields := strings.Fields(strings.TrimSpace(string(out)))
	return workerCommandUsesConfig(fields, configPath)
}

func workerCommandUsesConfig(args []string, targetConfigPath string) bool {
	if len(args) < 3 || !isNexusWorkerExecutable(args[0]) || args[1] != "worker" {
		return false
	}
	return workerConfigArgMatches(args[2], targetConfigPath)
}

func isNexusWorkerExecutable(arg string) bool {
	arg = strings.Trim(strings.TrimSpace(arg), `"'`)
	if arg == "" {
		return false
	}
	base := filepath.Base(arg)
	return base == "nexus-trade-bot" || base == "nexus-trade-bot.exe"
}

func workerConfigArgMatches(arg, targetConfigPath string) bool {
	arg = strings.Trim(strings.TrimSpace(arg), `"'`)
	if arg == "" || targetConfigPath == "" {
		return false
	}
	if filepath.Clean(arg) == filepath.Clean(targetConfigPath) {
		return true
	}
	argAbsPath, err := filepath.Abs(arg)
	if err != nil {
		return false
	}
	if resolvedPath, err := filepath.EvalSymlinks(argAbsPath); err == nil {
		argAbsPath = resolvedPath
	}
	return filepath.Clean(argAbsPath) == filepath.Clean(targetConfigPath)
}

func appendMissingPIDs(dst []int, src []int, exclude ...int) []int {
	seen := make(map[int]struct{}, len(dst)+len(exclude))
	for _, pid := range dst {
		if pid > 0 {
			seen[pid] = struct{}{}
		}
	}
	for _, pid := range exclude {
		if pid > 0 {
			seen[pid] = struct{}{}
		}
	}
	for _, pid := range src {
		if pid <= 0 {
			continue
		}
		if _, ok := seen[pid]; ok {
			continue
		}
		dst = append(dst, pid)
		seen[pid] = struct{}{}
	}
	return dst
}

func stopExtraWorkerPIDs(pids []int) {
	pids = appendMissingPIDs(nil, pids)
	if len(pids) == 0 {
		return
	}
	for _, pid := range pids {
		_ = signalProcessGroup(pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allStopped := true
		for _, pid := range pids {
			if processAlive(pid) {
				allStopped = false
				break
			}
		}
		if allStopped {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	for _, pid := range pids {
		if processAlive(pid) {
			_ = signalProcessGroup(pid, syscall.SIGKILL)
		}
	}
}

func (s *consoleServer) loadRobots() error {
	data, err := os.ReadFile(s.robotsPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var robots []*robotDefinition
	if err := json.Unmarshal(data, &robots); err != nil {
		return err
	}
	for _, robot := range robots {
		if robot == nil || strings.TrimSpace(robot.ID) == "" {
			continue
		}
		if robot.Config != nil && robot.AccountID != "" {
			_, _ = s.applyAccountToRobotConfig(robot.AccountID, robot.Config)
		}
		ensureRobotOrderTag(robot)
		s.robots[robot.ID] = robot
	}
	return nil
}

func (s *consoleServer) saveRobotsLocked() error {
	items := make([]*robotDefinition, 0, len(s.robots))
	for _, robot := range s.robots {
		clone := *robot
		clone.Config = publicConfig(robot.Config)
		items = append(items, &clone)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.robotsPath, data, 0600)
}

func (s *consoleServer) normalizeRobotConfig(cfg *config.Config) (*config.Config, error) {
	if cfg == nil {
		cfg = cloneConfig(s.baseConfig)
	}
	if cfg.Trading.Symbol != "" {
		cfg.Trading.Symbol = normalizeTradingSymbol(cfg.Trading.Symbol)
	}
	cfg.App.MarketType = normalizeMarketTypeParam(cfg.App.MarketType)
	if cfg.App.MarketType == "spot" {
		cfg.Trading.Direction = "long"
	}
	if err := applyHardcodedRobotDefaults(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (s *consoleServer) applyAccountToRobotConfig(accountID string, cfg *config.Config) (*accountProfile, error) {
	s.mu.RLock()
	account, ok := s.accounts[accountID]
	if ok {
		account = cloneAccountProfile(account)
	}
	s.mu.RUnlock()
	if !ok || account == nil {
		return nil, fmt.Errorf("必须先选择一个已保存账户")
	}
	return account, applyAccountConfigToConfig(cfg, account)
}

func (s *consoleServer) ensureNoConflictingRobotLocked(id string, accountID string, cfg *config.Config) error {
	scope := robotRuntimeScope(accountID, cfg)
	if scope == "" {
		return nil
	}
	for otherID, other := range s.robots {
		if otherID == id || other == nil {
			continue
		}
		if robotRuntimeScope(other.AccountID, other.Config) != scope {
			continue
		}
		name := strings.TrimSpace(other.Name)
		if name == "" {
			name = other.ID
		}
		symbol := ""
		if cfg != nil {
			symbol = normalizeTradingSymbol(cfg.Trading.Symbol)
		}
		return fmt.Errorf("同一账户/交易所/市场/交易对只能保留一个机器人，否则暂停或删除时会互相撤单。请先停止并删除冲突机器人：%s (%s, %s)", name, other.ID, symbol)
	}
	return nil
}

func (s *consoleServer) ensureNoAccountUpdateConflictsLocked(accountID string, exchangeName string) error {
	seen := make(map[string]*robotDefinition)
	for _, robot := range s.robots {
		if robot == nil || robot.Config == nil {
			continue
		}
		cfg := robot.Config
		if robot.AccountID == accountID {
			cfg = cloneConfig(robot.Config)
			cfg.App.CurrentExchange = exchangeName
		}
		scope := robotRuntimeScope(robot.AccountID, cfg)
		if scope == "" {
			continue
		}
		if other, exists := seen[scope]; exists && other != nil {
			return fmt.Errorf("同一账户/交易所/市场/交易对只能保留一个机器人，否则暂停或删除时会互相撤单。请先停止并删除冲突机器人：%s (%s) 与 %s (%s)", robotDisplayName(other), other.ID, robotDisplayName(robot), robot.ID)
		}
		seen[scope] = robot
	}
	return nil
}

func robotRuntimeScope(accountID string, cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	accountID = strings.TrimSpace(accountID)
	exchangeName := strings.ToLower(strings.TrimSpace(cfg.App.CurrentExchange))
	marketType := normalizeMarketTypeParam(cfg.App.MarketType)
	symbol := normalizeTradingSymbol(cfg.Trading.Symbol)
	if accountID == "" || exchangeName == "" || marketType == "" || symbol == "" {
		return ""
	}
	return accountID + "|" + exchangeName + "|" + marketType + "|" + symbol
}

func robotDisplayName(robot *robotDefinition) string {
	if robot == nil {
		return ""
	}
	name := strings.TrimSpace(robot.Name)
	if name == "" {
		return robot.ID
	}
	return name
}

func autoRobotName(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	symbol := formatTradingSymbolForName(cfg.Trading.Symbol)
	market := "合约"
	if strings.EqualFold(strings.TrimSpace(cfg.App.MarketType), "spot") {
		market = "现货"
	}
	direction := "做多"
	switch strings.ToLower(strings.TrimSpace(cfg.Trading.Direction)) {
	case "short":
		direction = "做空"
	case "neutral":
		direction = "中性"
	}
	if symbol == "" {
		symbol = "未设置交易对"
	}
	return symbol + " · " + market + " · " + direction
}

func formatTradingSymbolForName(symbol string) string {
	symbol = normalizeTradingSymbol(symbol)
	for _, quote := range []string{"USDT", "USDC", "USD"} {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			return symbol[:len(symbol)-len(quote)] + "/" + quote
		}
	}
	return symbol
}

func applyHardcodedRobotDefaults(cfg *config.Config) error {
	cfg.App.MarketType = normalizeMarketTypeParam(cfg.App.MarketType)
	if cfg.App.MarketType == "spot" {
		cfg.Trading.Direction = "long"
	}
	mode := strings.ToLower(strings.TrimSpace(cfg.Trading.Mode))
	if mode == "" {
		cfg.Trading.Mode = "normal"
		mode = "normal"
	} else {
		cfg.Trading.Mode = mode
	}
	if mode == "classic" {
		cfg.App.MarketType = "futures"
		cfg.Trading.Direction = "neutral"
	}
	if len(cfg.RiskControl.MonitorSymbols) == 0 {
		cfg.RiskControl.MonitorSymbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"}
	}
	if mode == "classic" && strings.EqualFold(strings.TrimSpace(cfg.App.CurrentExchange), "hyperliquid") {
		return fmt.Errorf("经典网格需要 neutral 双向挂单，Hyperliquid 合约暂不支持该模式")
	}
	cfg.RiskControl.Interval = hardcodedRiskInterval
	cfg.RiskControl.VolumeMultiplier = hardcodedRiskVolumeMultiplier
	cfg.RiskControl.AverageWindow = hardcodedRiskAverageWindow
	cfg.RiskControl.RecoveryThreshold = hardcodedRiskRecoveryThreshold
	cfg.ContractRisk.Enabled = hardcodedContractRiskEnabled
	cfg.ContractRisk.MaxMarginUsagePct = hardcodedMaxMarginUsagePct
	cfg.ContractRisk.MaxUnrealizedLossPct = hardcodedMaxUnrealizedLossPct
	cfg.ContractRisk.MaxAccountDrawdownPct = hardcodedMaxAccountDrawdownPct
	cfg.ContractRisk.LiquidationGuard.Enabled = true
	cfg.ContractRisk.LiquidationGuard.MinDistancePct = hardcodedLiquidationGuardPct

	cfg.System.LogLevel = hardcodedSystemLogLevel
	cfg.System.CancelOnExit = hardcodedSystemCancelOnExit

	cfg.Timing.WebSocketReconnectDelay = hardcodedWSReconnectDelay
	cfg.Timing.WebSocketWriteWait = hardcodedWSWriteWait
	cfg.Timing.WebSocketPongWait = hardcodedWSPongWait
	cfg.Timing.WebSocketPingInterval = hardcodedWSPingInterval
	cfg.Timing.ListenKeyKeepAliveInterval = hardcodedListenKeyKeepAlive
	cfg.Timing.PriceSendInterval = hardcodedPriceSendInterval
	cfg.Timing.RateLimitRetryDelay = hardcodedRateLimitRetryDelay
	cfg.Timing.OrderRetryDelay = hardcodedOrderRetryDelay
	cfg.Timing.PricePollInterval = hardcodedPricePollInterval
	cfg.Timing.StatusPrintInterval = hardcodedStatusPrintInterval
	cfg.Timing.OrderCleanupInterval = hardcodedOrderCleanupInterval

	cfg.Trading.BuyWindowSize = hardcodedBuyWindowSize
	cfg.Trading.SellWindowSize = hardcodedSellWindowSize
	if mode == "classic" {
		cfg.Trading.BuyWindowSize = config.ClassicGridWindowSize
		cfg.Trading.SellWindowSize = config.ClassicGridWindowSize
	}
	cfg.Trading.ReconcileInterval = hardcodedReconcileInterval
	cfg.Trading.OrderCleanupThreshold = hardcodedOrderCleanupThreshold
	if mode == "classic" {
		cfg.Trading.OrderCleanupThreshold = config.ClassicGridWindowSize * 2
	}
	cfg.Trading.CleanupBatchSize = hardcodedCleanupBatchSize
	cfg.Trading.MarginLockDurationSec = hardcodedMarginLockSeconds
	cfg.Trading.PositionSafetyCheck = hardcodedPositionSafetyCheck
	return nil
}

func (s *consoleServer) loadAccounts() error {
	data, err := os.ReadFile(s.accountsPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var accounts []*accountProfile
	if err := json.Unmarshal(data, &accounts); err != nil {
		return err
	}
	for _, account := range accounts {
		if account == nil || strings.TrimSpace(account.ID) == "" {
			continue
		}
		account.Config = normalizeAccountExchangeConfig(account.Config)
		s.accounts[account.ID] = account
	}
	return nil
}

func (s *consoleServer) saveAccountsLocked() error {
	items := make([]*accountProfile, 0, len(s.accounts))
	for _, account := range s.accounts {
		items = append(items, cloneAccountProfile(account))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.accountsPath, data, 0600)
}

func (s *consoleServer) collectAccounts() []*accountProfile {
	s.mu.RLock()
	items := make([]*accountProfile, 0, len(s.accounts))
	for _, account := range s.accounts {
		items = append(items, cloneAccountProfile(account))
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items
}

func (s *consoleServer) collectAccountViews() []accountView {
	accounts := s.collectAccounts()
	items := make([]accountView, 0, len(accounts))
	for _, account := range accounts {
		items = append(items, publicAccountView(account))
	}
	return items
}

func publicAccountView(account *accountProfile) accountView {
	if account == nil {
		return accountView{}
	}
	return accountView{
		ID:        account.ID,
		Name:      account.Name,
		Exchange:  account.Exchange,
		Config:    publicExchangeConfig(account.Config),
		CreatedAt: account.CreatedAt,
		UpdatedAt: account.UpdatedAt,
	}
}

func publicExchangeConfig(cfg config.ExchangeConfig) config.ExchangeConfig {
	if strings.TrimSpace(cfg.APIKey) != "" {
		cfg.APIKey = secretMask
	}
	if strings.TrimSpace(cfg.SecretKey) != "" {
		cfg.SecretKey = secretMask
	}
	if strings.TrimSpace(cfg.Passphrase) != "" {
		cfg.Passphrase = secretMask
	}
	return cfg
}

func publicConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		return nil
	}
	clone := cloneConfig(cfg)
	if clone.Exchanges != nil {
		for name, exchangeCfg := range clone.Exchanges {
			clone.Exchanges[name] = publicExchangeConfig(exchangeCfg)
		}
	}
	return clone
}

func (s *consoleServer) collectAccountBalances() []accountBalanceView {
	accounts := s.collectAccounts()
	items := make([]accountBalanceView, 0, len(accounts))
	now := time.Now()
	for _, account := range accounts {
		s.mu.RLock()
		cached, ok := s.balanceCache[account.ID]
		s.mu.RUnlock()
		if ok && cached.AccountUpdatedAt.Equal(account.UpdatedAt) && now.Before(cached.ExpiresAt) {
			items = append(items, cached.Value)
			continue
		}

		value := s.fetchAccountBalance(account)
		s.mu.Lock()
		s.balanceCache[account.ID] = accountBalanceCache{
			Value:            value,
			AccountUpdatedAt: account.UpdatedAt,
			ExpiresAt:        time.Now().Add(10 * time.Second),
		}
		s.mu.Unlock()
		items = append(items, value)
	}
	return items
}

func (s *consoleServer) fetchAccountBalance(account *accountProfile) accountBalanceView {
	view := accountBalanceView{
		ID:        account.ID,
		Name:      account.Name,
		Exchange:  account.Exchange,
		FetchedAt: time.Now(),
	}
	symbol := "BTCUSDT"
	if s.baseConfig != nil && s.baseConfig.Trading.Symbol != "" {
		symbol = normalizeTradingSymbol(s.baseConfig.Trading.Symbol)
	}
	cfg := accountConfig(account.Exchange, account.Config, symbol, "futures")
	ex, err := exchange.NewExchange(cfg)
	if err != nil {
		cfg = accountConfig(account.Exchange, account.Config, symbol, "spot")
		ex, err = exchange.NewExchange(cfg)
		if err != nil {
			view.Error = friendlyErrorMessage(err)
			return view
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	balance, err := ex.GetAccount(ctx)
	if err != nil {
		if cfg.App.MarketType == "spot" {
			view.Error = friendlyErrorMessage(err)
			return view
		}
		cfg = accountConfig(account.Exchange, account.Config, symbol, "spot")
		ex, err = exchange.NewExchange(cfg)
		if err != nil {
			view.Error = friendlyErrorMessage(err)
			return view
		}
		balance, err = ex.GetAccount(ctx)
		if err != nil {
			view.Error = friendlyErrorMessage(err)
			return view
		}
	}
	view.Balance = balance.TotalWalletBalance
	view.AvailableBalance = balance.AvailableBalance
	view.MarginBalance = balance.TotalMarginBalance
	view.QuoteAsset = ex.GetQuoteAsset()
	return view
}

func (s *consoleServer) createAccount(payload accountPayload) (*accountProfile, error) {
	if strings.TrimSpace(payload.Name) == "" {
		return nil, fmt.Errorf("账户名称不能为空")
	}
	exchangeName := config.NormalizeExchangeName(payload.Exchange)
	if exchangeName == "" {
		return nil, fmt.Errorf("交易所不能为空")
	}
	payload.Exchange = exchangeName
	payload.Config = normalizeAccountExchangeConfig(payload.Config)
	if strings.TrimSpace(payload.Config.APIKey) == "" || strings.TrimSpace(payload.Config.SecretKey) == "" {
		return nil, fmt.Errorf("API Key 和 Secret Key 不能为空")
	}
	if requiresPassphrase(payload.Exchange) && strings.TrimSpace(payload.Config.Passphrase) == "" {
		return nil, fmt.Errorf("%s 需要填写 Passphrase", strings.ToUpper(payload.Exchange))
	}
	if err := validateAccountPayload(payload); err != nil {
		return nil, err
	}
	id := makeRobotID(payload.Name)
	now := time.Now()
	account := &accountProfile{ID: id, Name: strings.TrimSpace(payload.Name), Exchange: payload.Exchange, Config: payload.Config, CreatedAt: now, UpdatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[id] = account
	if err := s.saveAccountsLocked(); err != nil {
		delete(s.accounts, id)
		return nil, err
	}
	return account, nil
}

func (s *consoleServer) updateAccount(id string, payload accountPayload) (*accountProfile, error) {
	if strings.TrimSpace(payload.Name) == "" {
		return nil, fmt.Errorf("账户名称不能为空")
	}
	exchangeName := config.NormalizeExchangeName(payload.Exchange)
	if exchangeName == "" {
		return nil, fmt.Errorf("交易所不能为空")
	}
	payload.Exchange = exchangeName
	s.mu.RLock()
	existing, exists := s.accounts[id]
	var existingConfig config.ExchangeConfig
	var existingExchange string
	if exists && existing != nil {
		existingConfig = existing.Config
		existingExchange = existing.Exchange
	}
	s.mu.RUnlock()
	if !exists || existing == nil {
		return nil, fmt.Errorf("账户不存在")
	}
	payload.Config = normalizeAccountExchangeConfig(mergeExistingSecrets(payload.Config, existingConfig, payload.Exchange == existingExchange))
	if strings.TrimSpace(payload.Config.APIKey) == "" || strings.TrimSpace(payload.Config.SecretKey) == "" {
		return nil, fmt.Errorf("API Key 和 Secret Key 不能为空")
	}
	if requiresPassphrase(payload.Exchange) && strings.TrimSpace(payload.Config.Passphrase) == "" {
		return nil, fmt.Errorf("%s 需要填写 Passphrase", strings.ToUpper(payload.Exchange))
	}
	if err := validateAccountPayload(payload); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	account, ok := s.accounts[id]
	if !ok {
		return nil, fmt.Errorf("账户不存在")
	}
	if err := s.ensureNoAccountUpdateConflictsLocked(id, payload.Exchange); err != nil {
		return nil, err
	}
	account.Name = strings.TrimSpace(payload.Name)
	account.Exchange = payload.Exchange
	account.Config = payload.Config
	account.UpdatedAt = time.Now()
	for _, robot := range s.robots {
		if robot.AccountID != id {
			continue
		}
		if err := applyAccountConfigToRobot(robot, account); err != nil {
			return nil, err
		}
		robot.UpdatedAt = time.Now()
		if err := config.SaveConfig(robot.ConfigPath, robot.Config); err != nil {
			return nil, fmt.Errorf("同步机器人 %s 的账户配置失败: %w", robot.Name, err)
		}
	}
	if err := s.saveAccountsLocked(); err != nil {
		return nil, err
	}
	if err := s.saveRobotsLocked(); err != nil {
		return nil, err
	}
	delete(s.balanceCache, id)
	s.clearPNLCacheForAccount(id)
	s.clearMetricCacheForAccountLocked(id)
	return account, nil
}

func applyAccountConfigToRobot(robot *robotDefinition, account *accountProfile) error {
	if robot == nil || robot.Config == nil {
		return fmt.Errorf("机器人配置不存在")
	}
	return applyAccountConfigToConfig(robot.Config, account)
}

func applyAccountConfigToConfig(cfg *config.Config, account *accountProfile) error {
	if cfg == nil {
		return fmt.Errorf("机器人配置不存在")
	}
	if account == nil {
		return fmt.Errorf("账户不存在")
	}
	exchangeName := config.NormalizeExchangeName(account.Exchange)
	if exchangeName == "" {
		return fmt.Errorf("不支持的交易所: %s", account.Exchange)
	}
	cfg.App.CurrentExchange = exchangeName
	if cfg.Exchanges == nil {
		cfg.Exchanges = make(map[string]config.ExchangeConfig)
	}
	cfg.Exchanges[exchangeName] = normalizeAccountExchangeConfig(account.Config)
	return nil
}

func normalizeAccountExchangeConfig(cfg config.ExchangeConfig) config.ExchangeConfig {
	cfg = trimExchangeConfig(cfg)
	if cfg.FeeRate == 0 {
		cfg.FeeRate = config.DefaultFeeRate
	}
	return cfg
}

func trimExchangeConfig(cfg config.ExchangeConfig) config.ExchangeConfig {
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.SecretKey = strings.TrimSpace(cfg.SecretKey)
	cfg.Passphrase = strings.TrimSpace(cfg.Passphrase)
	return cfg
}

func mergeExistingSecrets(next, existing config.ExchangeConfig, sameExchange bool) config.ExchangeConfig {
	next = trimExchangeConfig(next)
	if !sameExchange {
		return next
	}
	if isMaskedSecret(next.APIKey) {
		next.APIKey = existing.APIKey
	}
	if isMaskedSecret(next.SecretKey) {
		next.SecretKey = existing.SecretKey
	}
	if isMaskedSecret(next.Passphrase) {
		next.Passphrase = existing.Passphrase
	}
	return next
}

func isMaskedSecret(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == secretMask
}

func strongEnoughPassword(password string) bool {
	trimmed := strings.TrimSpace(password)
	if len(trimmed) < 12 || strings.EqualFold(trimmed, "admin") {
		return false
	}
	return true
}

func requiresPassphrase(exchangeName string) bool {
	switch strings.ToLower(exchangeName) {
	case "bitget", "okx":
		return true
	default:
		return false
	}
}

func validateAccountPayload(payload accountPayload) error {
	exchangeName := config.NormalizeExchangeName(payload.Exchange)
	if exchangeName == "" {
		return fmt.Errorf("不支持的交易所: %s", payload.Exchange)
	}
	cfg := accountConfig(exchangeName, payload.Config, "BTCUSDT", "futures")
	ex, err := exchange.NewExchange(cfg)
	if err != nil {
		spotCfg := accountConfig(exchangeName, payload.Config, "BTCUSDT", "spot")
		spotEx, spotErr := exchange.NewExchange(spotCfg)
		if spotErr != nil {
			return fmt.Errorf("账户校验失败: %s", friendlyErrorMessage(err))
		}
		ex = spotEx
		cfg = spotCfg
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := ex.GetAccount(ctx); err != nil {
		if cfg.App.MarketType != "spot" {
			spotCfg := accountConfig(exchangeName, payload.Config, "BTCUSDT", "spot")
			spotEx, spotErr := exchange.NewExchange(spotCfg)
			if spotErr == nil {
				spotCtx, spotCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer spotCancel()
				if _, spotAccountErr := spotEx.GetAccount(spotCtx); spotAccountErr == nil {
					return nil
				}
			}
		}
		return fmt.Errorf("API Key 校验失败: %s", friendlyErrorMessage(err))
	}
	return nil
}

func collectPublicSymbols(marketType string) []string {
	symbolSet := make(map[string]bool)
	for _, symbol := range fallbackSymbols() {
		symbolSet[symbol] = true
	}
	if marketType == "spot" {
		for _, symbol := range fetchBinanceSpotSymbols() {
			symbolSet[symbol] = true
		}
		for _, symbol := range fetchBitgetSpotSymbols() {
			symbolSet[symbol] = true
		}
		for _, symbol := range fetchBybitSpotSymbols() {
			symbolSet[symbol] = true
		}
		for _, symbol := range fetchOKXSpotSymbols() {
			symbolSet[symbol] = true
		}
		for _, symbol := range fetchGateSpotSymbols() {
			symbolSet[symbol] = true
		}
		for _, symbol := range fetchHyperliquidSpotSymbols() {
			symbolSet[symbol] = true
		}
	} else {
		for _, symbol := range fetchBinanceFuturesSymbols() {
			symbolSet[symbol] = true
		}
		for _, symbol := range fetchBitgetFuturesSymbols() {
			symbolSet[symbol] = true
		}
	}
	symbols := make([]string, 0, len(symbolSet))
	for symbol := range symbolSet {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)
	return symbols
}

func fetchBinanceSpotSymbols() []string {
	var payload struct {
		Symbols []struct {
			Symbol               string `json:"symbol"`
			Status               string `json:"status"`
			QuoteAsset           string `json:"quoteAsset"`
			IsSpotTradingAllowed bool   `json:"isSpotTradingAllowed"`
		} `json:"symbols"`
	}
	if err := fetchPublicJSON("https://api.binance.com/api/v3/exchangeInfo", &payload); err != nil {
		logger.Warn("⚠️ 获取 Binance 现货交易对失败: %s", publicFetchErrorMessage(err))
		return nil
	}
	var symbols []string
	for _, item := range payload.Symbols {
		if item.Status == "TRADING" && item.IsSpotTradingAllowed && (item.QuoteAsset == "USDT" || item.QuoteAsset == "USDC") {
			symbols = append(symbols, strings.ToUpper(item.Symbol))
		}
	}
	return symbols
}

func fetchBinanceFuturesSymbols() []string {
	var payload struct {
		Symbols []struct {
			Symbol       string `json:"symbol"`
			Status       string `json:"status"`
			ContractType string `json:"contractType"`
			QuoteAsset   string `json:"quoteAsset"`
		} `json:"symbols"`
	}
	if err := fetchPublicJSON("https://fapi.binance.com/fapi/v1/exchangeInfo", &payload); err != nil {
		logger.Warn("⚠️ 获取 Binance 合约交易对失败: %s", publicFetchErrorMessage(err))
		return nil
	}
	var symbols []string
	for _, item := range payload.Symbols {
		if item.Status == "TRADING" && item.ContractType == "PERPETUAL" && (item.QuoteAsset == "USDT" || item.QuoteAsset == "USDC") {
			symbols = append(symbols, strings.ToUpper(item.Symbol))
		}
	}
	return symbols
}

func fetchBitgetSpotSymbols() []string {
	var payload struct {
		Code string `json:"code"`
		Data []struct {
			Symbol    string `json:"symbol"`
			QuoteCoin string `json:"quoteCoin"`
			Status    string `json:"status"`
		} `json:"data"`
	}
	if err := fetchPublicJSON("https://api.bitget.com/api/v2/spot/public/symbols", &payload); err != nil {
		logger.Warn("⚠️ 获取 Bitget 现货交易对失败: %v", err)
		return nil
	}
	if payload.Code != "" && payload.Code != "00000" {
		logger.Warn("⚠️ 获取 Bitget 现货交易对失败: code=%s", payload.Code)
		return nil
	}
	var symbols []string
	for _, item := range payload.Data {
		if item.Status == "online" && (item.QuoteCoin == "USDT" || item.QuoteCoin == "USDC") {
			if symbol := normalizeTradingSymbol(item.Symbol); symbol != "" {
				symbols = append(symbols, symbol)
			}
		}
	}
	return symbols
}

func fetchBybitSpotSymbols() []string {
	var payload struct {
		RetCode int `json:"retCode"`
		Result  struct {
			List []struct {
				Symbol    string `json:"symbol"`
				QuoteCoin string `json:"quoteCoin"`
				Status    string `json:"status"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := fetchPublicJSON("https://api.bybit.com/v5/market/instruments-info?category=spot", &payload); err != nil {
		logger.Warn("⚠️ 获取 Bybit 现货交易对失败: %v", err)
		return nil
	}
	if payload.RetCode != 0 {
		logger.Warn("⚠️ 获取 Bybit 现货交易对失败: code=%d", payload.RetCode)
		return nil
	}
	var symbols []string
	for _, item := range payload.Result.List {
		if item.Status == "Trading" && (item.QuoteCoin == "USDT" || item.QuoteCoin == "USDC") {
			symbols = append(symbols, strings.ToUpper(item.Symbol))
		}
	}
	return symbols
}

func fetchOKXSpotSymbols() []string {
	var payload struct {
		Code string `json:"code"`
		Data []struct {
			InstID   string `json:"instId"`
			QuoteCcy string `json:"quoteCcy"`
			State    string `json:"state"`
		} `json:"data"`
	}
	if err := fetchPublicJSON("https://www.okx.com/api/v5/public/instruments?instType=SPOT", &payload); err != nil {
		logger.Warn("⚠️ 获取 OKX 现货交易对失败: %v", err)
		return nil
	}
	if payload.Code != "" && payload.Code != "0" {
		logger.Warn("⚠️ 获取 OKX 现货交易对失败: code=%s", payload.Code)
		return nil
	}
	var symbols []string
	for _, item := range payload.Data {
		if item.State == "live" && (item.QuoteCcy == "USDT" || item.QuoteCcy == "USDC") {
			if symbol := normalizeTradingSymbol(item.InstID); symbol != "" {
				symbols = append(symbols, symbol)
			}
		}
	}
	return symbols
}

func fetchGateSpotSymbols() []string {
	var items []struct {
		ID          string `json:"id"`
		Quote       string `json:"quote"`
		TradeStatus string `json:"trade_status"`
	}
	if err := fetchPublicJSON("https://api.gateio.ws/api/v4/spot/currency_pairs", &items); err != nil {
		logger.Warn("⚠️ 获取 Gate 现货交易对失败: %v", err)
		return nil
	}
	var symbols []string
	for _, item := range items {
		if item.TradeStatus == "tradable" && (item.Quote == "USDT" || item.Quote == "USDC") {
			if symbol := normalizeTradingSymbol(item.ID); symbol != "" {
				symbols = append(symbols, symbol)
			}
		}
	}
	return symbols
}

func fetchHyperliquidSpotSymbols() []string {
	var payload struct {
		Universe []struct {
			Name string `json:"name"`
		} `json:"universe"`
	}
	if err := postPublicJSON("https://api.hyperliquid.xyz/info", map[string]string{"type": "spotMeta"}, &payload); err != nil {
		logger.Warn("⚠️ 获取 Hyperliquid 现货交易对失败: %v", err)
		return nil
	}
	var symbols []string
	for _, item := range payload.Universe {
		name := strings.ToUpper(item.Name)
		if strings.HasSuffix(name, "/USDC") {
			if symbol := normalizeTradingSymbol(name); symbol != "" {
				symbols = append(symbols, symbol)
			}
		}
	}
	return symbols
}

func fetchBitgetFuturesSymbols() []string {
	var symbols []string
	for _, productType := range []string{"USDT-FUTURES", "USDC-FUTURES"} {
		var payload struct {
			Code string `json:"code"`
			Data []struct {
				Symbol       string `json:"symbol"`
				SymbolStatus string `json:"symbolStatus"`
				SymbolType   string `json:"symbolType"`
			} `json:"data"`
		}
		url := "https://api.bitget.com/api/v2/mix/market/contracts?productType=" + productType
		if err := fetchPublicJSON(url, &payload); err != nil {
			logger.Warn("⚠️ 获取 Bitget %s 合约交易对失败: %v", productType, err)
			continue
		}
		if payload.Code != "" && payload.Code != "00000" {
			logger.Warn("⚠️ 获取 Bitget %s 合约交易对失败: code=%s", productType, payload.Code)
			continue
		}
		for _, item := range payload.Data {
			if item.SymbolStatus != "normal" || item.SymbolType != "perpetual" {
				continue
			}
			if symbol := normalizeTradingSymbol(item.Symbol); symbol != "" {
				symbols = append(symbols, symbol)
			}
		}
	}
	return symbols
}

func fetchPublicJSON(url string, dst interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(dst)
}

func postPublicJSON(url string, payload interface{}, dst interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(dst)
}

func publicFetchErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "status=451") || strings.Contains(lower, "restricted location") {
		return "服务器所在地区被交易所限制访问，已使用内置交易对列表兜底。"
	}
	return msg
}

func fallbackSymbols() []string {
	return []string{
		"1000PEPEUSDT", "AAVEUSDT", "ADAUSDT", "APTUSDT", "ARBUSDT", "ATOMUSDT", "AVAXUSDT",
		"BCHUSDT", "BNBUSDT", "BTCUSDT", "DOGEUSDT", "DOTUSDT", "ENAUSDT", "EOSUSDT",
		"ETCUSDT", "ETHUSDC", "ETHUSDT", "FILUSDT", "INJUSDT", "JUPUSDT", "LINKUSDT",
		"LTCUSDT", "MATICUSDT", "NEARUSDT", "OPUSDT", "ORDIUSDT", "PEPEUSDT", "POLUSDT",
		"PYTHUSDT", "SEIUSDT", "SHIBUSDT", "SOLUSDT", "SUIUSDT", "TIAUSDT", "TONUSDT",
		"TRXUSDT", "UNIUSDT", "WIFUSDT", "WLDUSDT", "XRPUSDT",
	}
}

func accountConfig(exchangeName string, exchangeConfig config.ExchangeConfig, symbol string, marketType string) *config.Config {
	cfg := &config.Config{}
	exchangeConfig = normalizeAccountExchangeConfig(exchangeConfig)
	exchangeName = config.NormalizeExchangeName(exchangeName)
	cfg.App.CurrentExchange = exchangeName
	cfg.App.MarketType = normalizeMarketTypeParam(marketType)
	cfg.Exchanges = map[string]config.ExchangeConfig{
		exchangeName: exchangeConfig,
	}
	cfg.Trading.Symbol = normalizeTradingSymbol(symbol)
	cfg.Trading.Direction = "long"
	cfg.Trading.PriceInterval = 1
	cfg.Trading.OrderQuantity = 20
	cfg.Trading.MinOrderValue = 20
	cfg.Trading.BuyWindowSize = hardcodedBuyWindowSize
	cfg.Trading.SellWindowSize = hardcodedSellWindowSize
	cfg.Trading.ReconcileInterval = hardcodedReconcileInterval
	cfg.Trading.OrderCleanupThreshold = hardcodedOrderCleanupThreshold
	cfg.Trading.CleanupBatchSize = hardcodedCleanupBatchSize
	cfg.Trading.MarginLockDurationSec = hardcodedMarginLockSeconds
	cfg.Trading.PositionSafetyCheck = hardcodedPositionSafetyCheck
	_ = applyHardcodedRobotDefaults(cfg)
	return cfg
}

func normalizeMarketTypeParam(marketType string) string {
	switch strings.ToLower(strings.TrimSpace(marketType)) {
	case "spot":
		return "spot"
	default:
		return "futures"
	}
}

func normalizeTradingSymbol(symbol string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(symbol), "/", ""))
}

func (s *consoleServer) deleteAccount(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, robot := range s.robots {
		if robot.AccountID == id {
			return fmt.Errorf("该账户正在被机器人 %s 使用，无法删除", robot.Name)
		}
	}
	if _, ok := s.accounts[id]; !ok {
		return fmt.Errorf("账户不存在")
	}
	delete(s.accounts, id)
	delete(s.balanceCache, id)
	s.clearPNLCacheForAccount(id)
	s.clearMetricCacheForAccountLocked(id)
	return s.saveAccountsLocked()
}

func makeRobotID(name string) string {
	trimmed := strings.TrimSpace(strings.ToLower(name))
	trimmed = strings.ReplaceAll(trimmed, " ", "-")
	trimmed = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return -1
	}, trimmed)
	if trimmed == "" {
		trimmed = "robot"
	}
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return fmt.Sprintf("%s-%s", trimmed, hex.EncodeToString(buf))
}

func robotOrderTag(id string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(id)))
	return utils.NormalizeOrderTag(hex.EncodeToString(sum[:])[:6])
}

func ensureRobotOrderTag(robot *robotDefinition) {
	if robot == nil || robot.Config == nil {
		return
	}
	if tag := utils.NormalizeOrderTag(robot.Config.Trading.OrderTag); tag != "" {
		robot.Config.Trading.OrderTag = tag
		return
	}
	robot.Config.Trading.OrderTag = robotOrderTag(robot.ID)
}

func cloneConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		return nil
	}
	clone := *cfg
	if cfg.Exchanges != nil {
		clone.Exchanges = make(map[string]config.ExchangeConfig, len(cfg.Exchanges))
		for name, exchangeCfg := range cfg.Exchanges {
			clone.Exchanges[name] = exchangeCfg
		}
	}
	if cfg.RiskControl.MonitorSymbols != nil {
		clone.RiskControl.MonitorSymbols = append([]string(nil), cfg.RiskControl.MonitorSymbols...)
	}
	return &clone
}

func cloneAccountProfile(account *accountProfile) *accountProfile {
	if account == nil {
		return nil
	}
	clone := *account
	return &clone
}

func cloneRobotDefinition(robot *robotDefinition) *robotDefinition {
	if robot == nil {
		return nil
	}
	clone := *robot
	clone.Config = cloneConfig(robot.Config)
	return &clone
}

func (s *consoleServer) collectRobotViews() []robotView {
	s.mu.RLock()
	items := make([]*robotDefinition, 0, len(s.robots))
	for _, robot := range s.robots {
		items = append(items, cloneRobotDefinition(robot))
	}
	s.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	views := make([]robotView, len(items))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, robot := range items {
		i, robot := i, robot
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			views[i] = s.buildRobotView(robot)
		}()
	}
	wg.Wait()
	return views
}

func (s *consoleServer) collectRobotConfigPaths() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	paths := make(map[string]bool, len(s.robots))
	for _, robot := range s.robots {
		if robot.ConfigPath != "" {
			paths[filepath.Clean(robot.ConfigPath)] = true
		}
	}
	return paths
}

func (s *consoleServer) collectArchivedRobotMetrics(activeConfigPaths map[string]bool) []robotMetric {
	matches, err := filepath.Glob(filepath.Join(s.robotsDir, "*.stats.json"))
	if err != nil || len(matches) == 0 {
		return nil
	}
	sort.Strings(matches)
	metrics := make([]robotMetric, 0)
	for _, statsPath := range matches {
		configPath := strings.TrimSuffix(statsPath, ".stats.json")
		if activeConfigPaths[filepath.Clean(configPath)] {
			continue
		}
		statsSnapshot, err := tradestats.LoadWithLogFallback(statsPath, robotLogPath(configPath), 0)
		if err != nil {
			continue
		}
		todayStats := tradestats.Today(statsSnapshot)
		metrics = append(metrics, robotMetric{
			TodayRealizedPNL: todayStats.RealizedPNL,
			TotalRealizedPNL: statsSnapshot.TotalRealizedPNL,
			TodayVolume:      todayStats.Volume,
			TotalVolume:      statsSnapshot.TotalVolume,
			TodayFees:        feeRateForConfigPath(configPath) * todayStats.Volume,
			TotalFees:        feeRateForConfigPath(configPath) * statsSnapshot.TotalVolume,
		})
	}
	return metrics
}

func (s *consoleServer) buildRobotView(robot *robotDefinition) robotView {
	s.mu.Lock()
	view := s.buildRobotViewLocked(robot)
	s.mu.Unlock()
	view.Metric = s.fetchRobotMetric(robot)
	return view
}

func (s *consoleServer) buildRobotViewLocked(robot *robotDefinition) robotView {
	if robot != nil {
		s.refreshRobotProcessStateLocked(robot.ID)
	}
	view := robotView{
		ID:        robot.ID,
		Name:      robot.Name,
		Config:    publicConfig(robot.Config),
		AccountID: robot.AccountID,
		Status:    "stopped",
		UpdatedAt: robot.UpdatedAt,
	}
	if account, ok := s.accounts[robot.AccountID]; ok {
		accountView := publicAccountView(account)
		view.Account = &accountView
	}
	if proc, ok := s.processes[robot.ID]; ok {
		view.Status = proc.Status
		view.PID = proc.PID
		view.StartedAt = proc.StartedAt
		view.StoppedAt = proc.StoppedAt
		view.LastError = proc.LastError
		view.ExitReason = proc.ExitReason
	}
	return view
}

func (s *consoleServer) refreshRobotProcessStateLocked(id string) {
	if s.processes == nil {
		return
	}
	proc, ok := s.processes[id]
	if !ok || proc == nil {
		if pid, alive := s.runningPIDFromFile(id); alive {
			s.processes[id] = &robotProcess{
				Status:        "running",
				PID:           pid,
				StartedAt:     time.Now(),
				DesiredStatus: "running",
				LogPath:       filepath.Join(s.robotsDir, id+".log"),
				ExitReason:    "已接管现有 worker 进程",
			}
			return
		}
		return
	}
	if proc.Status == "running" && proc.PID > 0 && !s.isProcessAlive(proc.PID) {
		oldPID := proc.PID
		status := "stopped"
		reason := "worker 进程已退出"
		if proc.DesiredStatus == "paused" {
			status = "paused"
			reason = "已暂停"
		}
		proc.Status = status
		proc.DesiredStatus = status
		proc.StoppedAt = time.Now()
		proc.ExitReason = reason
		proc.PID = 0
		if proc.logFile != nil {
			_ = proc.logFile.Close()
			proc.logFile = nil
		}
		s.removeRobotPIDIfCurrent(id, oldPID)
	}
}

func (s *consoleServer) fetchRobotMetric(robot *robotDefinition) robotMetric {
	statsMetric := robotStatsMetric(robot.ConfigPath, 0)
	if !s.robotCurrentlyRunning(robot.ID) {
		if cached, ok := s.cachedRobotMetric(robot); ok {
			return mergeStatsMetric(cached, statsMetric)
		}
		return statsMetric
	}
	if cached, ok := s.cachedRobotMetric(robot); ok {
		return mergeStatsMetric(cached, statsMetric)
	}
	return s.fetchAndCacheRobotMetric(robot, statsMetric)
}

func (s *consoleServer) robotCurrentlyRunning(id string) bool {
	if s == nil || strings.TrimSpace(id) == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshRobotProcessStateLocked(id)
	proc, ok := s.processes[id]
	return ok && proc != nil && proc.Status == "running"
}

func (s *consoleServer) cachedRobotMetric(robot *robotDefinition) (robotMetric, bool) {
	if robot == nil {
		return robotMetric{}, false
	}
	now := time.Now()
	s.mu.RLock()
	cached, ok := s.metricCache[robot.ID]
	s.mu.RUnlock()
	if !ok || cached.ConfigPath != robot.ConfigPath || !cached.RobotUpdatedAt.Equal(robot.UpdatedAt) || !now.Before(cached.ExpiresAt) {
		return robotMetric{}, false
	}
	return cached.Value, true
}

func (s *consoleServer) fetchAndCacheRobotMetric(robot *robotDefinition, statsMetric robotMetric) robotMetric {
	ex, err := exchange.NewExchange(robot.Config)
	if err != nil {
		statsMetric.Error = friendlyErrorMessage(err)
		s.storeRobotMetricCache(robot, statsMetric)
		return statsMetric
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	account, err := ex.GetAccount(ctx)
	if err != nil {
		statsMetric.Error = friendlyErrorMessage(err)
	} else {
		statsMetric.Balance = account.TotalWalletBalance
		statsMetric.AvailableBalance = account.AvailableBalance
		statsMetric.MarginBalance = account.TotalMarginBalance
	}
	currentPrice, priceErr := ex.GetLatestPrice(ctx, robot.Config.Trading.Symbol)
	if currentPrice > 0 {
		statsMetric = mergeStatsMetric(statsMetric, robotStatsMetric(robot.ConfigPath, currentPrice))
		statsMetric.PriceChangePct = s.priceChangePct(robot, currentPrice)
	}
	openOrders, err := ex.GetOpenOrders(ctx, robot.Config.Trading.Symbol)
	if err != nil && statsMetric.Error == "" {
		statsMetric.Error = friendlyErrorMessage(err)
	} else if err == nil {
		statsMetric.OpenOrderCount = len(openOrders)
	}
	positions, err := ex.GetPositions(ctx, robot.Config.Trading.Symbol)
	if err != nil && statsMetric.Error == "" {
		statsMetric.Error = friendlyErrorMessage(err)
	} else if err == nil {
		statsMetric = applyExchangePositionsMetric(statsMetric, positions, currentPrice)
	}
	if pnlSummary, ok := s.exchangePNLSummary(ctx, robot, ex); ok {
		statsMetric = applyExchangePNLSummary(statsMetric, pnlSummary)
	}
	if currentPrice > 0 {
		statsMetric.CurrentPrice = currentPrice
	}
	statsMetric.QuoteAsset = ex.GetQuoteAsset()
	statsMetric.PriceError = friendlyErrorString(priceErr)
	s.storeRobotMetricCache(robot, statsMetric)
	return statsMetric
}

func (s *consoleServer) storeRobotMetricCache(robot *robotDefinition, metric robotMetric) {
	if robot == nil {
		return
	}
	s.mu.Lock()
	if s.metricCache != nil {
		s.metricCache[robot.ID] = robotMetricCache{
			Value:          metric,
			RobotUpdatedAt: robot.UpdatedAt,
			ConfigPath:     robot.ConfigPath,
			ExpiresAt:      time.Now().Add(robotMetricCacheTTL),
		}
	}
	s.mu.Unlock()
}

func applyExchangePositionsMetric(metric robotMetric, positions []*exchange.Position, currentPrice float64) robotMetric {
	var exchangePNL float64
	var latestPricePNL float64
	hasExchangePNL := false
	hasLatestPricePNL := false
	positionCount := 0
	metric.LongPosition = 0
	metric.ShortPosition = 0

	for _, pos := range positions {
		if pos == nil || math.Abs(pos.Size) <= 1e-12 {
			continue
		}
		positionCount++
		if pos.HasUnrealizedPNL {
			exchangePNL += pos.UnrealizedPNL
			hasExchangePNL = true
		}
		if pos.Size > 0 {
			metric.LongPosition += pos.Size
		} else {
			metric.ShortPosition += -pos.Size
		}
		if currentPrice > 0 && pos.EntryPrice > 0 {
			qty := math.Abs(pos.Size)
			if pos.Size > 0 {
				latestPricePNL += (currentPrice - pos.EntryPrice) * qty
			} else {
				latestPricePNL += (pos.EntryPrice - currentPrice) * qty
			}
			hasLatestPricePNL = true
		}
	}

	metric.NetPosition = metric.LongPosition - metric.ShortPosition
	metric.PositionCount = positionCount
	if hasExchangePNL {
		metric.UnrealizedPNL = exchangePNL
		metric.HasExchangeUnrealizedPNL = true
	} else if hasLatestPricePNL {
		metric.UnrealizedPNL = latestPricePNL
	}
	return metric
}

func applyExchangePNLSummary(metric robotMetric, summary exchange.PNLSummary) robotMetric {
	if summary.HasTotalRealizedPNL {
		metric.TotalRealizedPNL = summary.TotalRealizedPNL
		metric.HasExchangeTotalRealizedPNL = true
	}
	if summary.HasTodayRealizedPNL {
		metric.TodayRealizedPNL = summary.TodayRealizedPNL
		metric.HasExchangeTodayRealizedPNL = true
	}
	return metric
}

func (s *consoleServer) exchangePNLSummary(ctx context.Context, robot *robotDefinition, ex exchange.IExchange) (exchange.PNLSummary, bool) {
	provider, ok := ex.(exchange.PNLProvider)
	if !ok || robot == nil || robot.Config == nil {
		return exchange.PNLSummary{}, false
	}
	now := time.Now()
	loc := tradestats.TradingDayLocation()
	localNow := now.In(loc)
	todayStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, loc)
	startTime := now.AddDate(0, 0, -89)
	symbol := strings.ToUpper(strings.TrimSpace(robot.Config.Trading.Symbol))
	cacheKey := exchangePNLCacheKey(robot, symbol, tradestats.TradingDayKey(now))

	s.pnlCacheMu.Lock()
	if cached, ok := s.pnlCache[cacheKey]; ok && now.Before(cached.ExpiresAt) {
		s.pnlCacheMu.Unlock()
		return cached.Value, true
	}
	s.pnlCacheMu.Unlock()

	summary, err := provider.GetPNLSummary(ctx, symbol, startTime, now, todayStart)
	if err != nil || summary == nil {
		if err != nil {
			logger.Debug("ℹ️ 交易所盈亏汇总读取失败，将使用本地统计: %v", err)
		}
		return exchange.PNLSummary{}, false
	}
	s.pnlCacheMu.Lock()
	s.pnlCache[cacheKey] = pnlCacheEntry{Value: *summary, ExpiresAt: now.Add(30 * time.Second)}
	s.pnlCacheMu.Unlock()
	return *summary, true
}

func exchangePNLCacheKey(robot *robotDefinition, symbol, tradingDay string) string {
	if robot == nil || robot.Config == nil {
		return ""
	}
	return strings.Join([]string{
		strings.TrimSpace(robot.AccountID),
		strings.ToLower(strings.TrimSpace(robot.Config.App.CurrentExchange)),
		normalizeMarketTypeParam(robot.Config.App.MarketType),
		strings.ToUpper(strings.TrimSpace(symbol)),
		strings.TrimSpace(tradingDay),
	}, "|")
}

func (s *consoleServer) clearPNLCacheForAccount(accountID string) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return
	}
	s.pnlCacheMu.Lock()
	defer s.pnlCacheMu.Unlock()
	s.clearPNLCacheForAccountLocked(accountID)
}

func (s *consoleServer) clearPNLCacheForAccountLocked(accountID string) {
	for key := range s.pnlCache {
		if strings.HasPrefix(key, accountID+"|") {
			delete(s.pnlCache, key)
		}
	}
}

func (s *consoleServer) clearMetricCacheForAccountLocked(accountID string) {
	for id, robot := range s.robots {
		if robot != nil && robot.AccountID == accountID {
			delete(s.metricCache, id)
		}
	}
}

func (s *consoleServer) priceChangePct(robot *robotDefinition, currentPrice float64) float64 {
	if robot == nil || robot.Config == nil || currentPrice <= 0 {
		return 0
	}
	date := tradestats.TradingDayKey(time.Now())
	key := robot.ID + "|" + strings.ToUpper(strings.TrimSpace(robot.Config.Trading.Symbol))

	s.priceBaseMu.Lock()
	defer s.priceBaseMu.Unlock()

	store := priceBaselineStore{Items: make(map[string]priceBaselineRecord)}
	if data, err := os.ReadFile(s.priceBasePath); err == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &store)
	}
	if store.Items == nil {
		store.Items = make(map[string]priceBaselineRecord)
	}
	record := store.Items[key]
	if record.Date != date || record.Baseline <= 0 {
		record = priceBaselineRecord{Date: date, Baseline: currentPrice, UpdatedAt: time.Now()}
		store.Items[key] = record
		s.savePriceBaselineStore(store)
		return 0
	}
	record.UpdatedAt = time.Now()
	store.Items[key] = record
	s.savePriceBaselineStore(store)
	return (currentPrice - record.Baseline) / record.Baseline * 100
}

func (s *consoleServer) savePriceBaselineStore(store priceBaselineStore) {
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(s.priceBasePath, data, 0600); err == nil {
		_ = os.Chmod(s.priceBasePath, 0600)
	}
}

func robotStatsMetric(configPath string, markPrice float64) robotMetric {
	statsSnapshot, statsErr := tradestats.LoadWithLogFallback(tradestats.PathForConfig(configPath), robotLogPath(configPath), markPrice)
	todayStats := tradestats.Today(statsSnapshot)
	feeRate := feeRateForConfigPath(configPath)
	statsError := ""
	if statsErr != nil {
		statsError = friendlyErrorMessage(fmt.Errorf("读取交易统计失败: %w", statsErr))
	}
	return robotMetric{
		CurrentPrice:     statsSnapshot.LastMarkPrice,
		UnrealizedPNL:    statsSnapshot.UnrealizedPNL,
		TodayRealizedPNL: todayStats.RealizedPNL,
		TotalRealizedPNL: statsSnapshot.TotalRealizedPNL,
		TodayVolume:      todayStats.Volume,
		TotalVolume:      statsSnapshot.TotalVolume,
		TodayFees:        feeRate * todayStats.Volume,
		TotalFees:        feeRate * statsSnapshot.TotalVolume,
		Error:            statsError,
	}
}

func robotLogPath(configPath string) string {
	ext := filepath.Ext(configPath)
	if ext == "" {
		return configPath + ".log"
	}
	return strings.TrimSuffix(configPath, ext) + ".log"
}

func feeRateForConfigPath(configPath string) float64 {
	cfg, err := config.LoadConfig(configPath)
	if err != nil || cfg == nil {
		return config.DefaultFeeRate
	}
	exchangeCfg, ok := cfg.Exchanges[cfg.App.CurrentExchange]
	if !ok || exchangeCfg.FeeRate < 0 {
		return config.DefaultFeeRate
	}
	if exchangeCfg.FeeRate == 0 {
		return config.DefaultFeeRate
	}
	return exchangeCfg.FeeRate
}

func mergeStatsMetric(base, next robotMetric) robotMetric {
	if next.CurrentPrice > 0 {
		base.CurrentPrice = next.CurrentPrice
	}
	if next.HasExchangeUnrealizedPNL || !base.HasExchangeUnrealizedPNL {
		base.UnrealizedPNL = next.UnrealizedPNL
		base.HasExchangeUnrealizedPNL = next.HasExchangeUnrealizedPNL
	}
	if next.HasExchangeTodayRealizedPNL || !base.HasExchangeTodayRealizedPNL {
		base.TodayRealizedPNL = next.TodayRealizedPNL
		base.HasExchangeTodayRealizedPNL = next.HasExchangeTodayRealizedPNL
	}
	if next.HasExchangeTotalRealizedPNL || !base.HasExchangeTotalRealizedPNL {
		base.TotalRealizedPNL = next.TotalRealizedPNL
		base.HasExchangeTotalRealizedPNL = next.HasExchangeTotalRealizedPNL
	}
	base.TodayVolume = next.TodayVolume
	base.TotalVolume = next.TotalVolume
	base.TodayFees = next.TodayFees
	base.TotalFees = next.TotalFees
	if next.Error != "" {
		base.Error = next.Error
	}
	return base
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func friendlyErrorString(err error) string {
	if err == nil {
		return ""
	}
	return friendlyErrorMessage(err)
}

func friendlyRobotExitError(err error, logPath string) string {
	logTail := tailTextFile(logPath, 4096)
	if logTail != "" {
		return friendlyErrorMessage(errors.New(logTail))
	}
	return friendlyErrorMessage(err)
}

func tailTextFile(path string, maxBytes int) string {
	if strings.TrimSpace(path) == "" || maxBytes <= 0 {
		return ""
	}
	data, err := readTailBytes(path, maxBytes)
	if err != nil || len(data) == 0 {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		return line
	}
	return strings.TrimSpace(string(data))
}

func tailTextFileBytes(path string, maxBytes int) (string, error) {
	data, err := readTailBytes(path, maxBytes)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func readTailBytes(path string, maxBytes int) ([]byte, error) {
	if strings.TrimSpace(path) == "" || maxBytes <= 0 {
		return nil, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	size := info.Size()
	offset := int64(0)
	if size > int64(maxBytes) {
		offset = size - int64(maxBytes)
	}
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if newline := bytes.IndexByte(data, '\n'); newline >= 0 && newline+1 < len(data) {
			data = data[newline+1:]
		}
	}
	return data, nil
}

func friendlyErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	msg = strings.TrimPrefix(msg, "Error:")
	msg = strings.TrimSpace(msg)
	if strings.Contains(msg, "❌") {
		parts := strings.Split(msg, "❌")
		msg = parts[len(parts)-1]
	}
	msg = strings.TrimPrefix(strings.TrimSpace(msg), "[FATAL]")
	msg = strings.TrimPrefix(strings.TrimSpace(msg), "[ERROR]")
	msg = strings.TrimPrefix(strings.TrimSpace(msg), "ERROR")
	msg = strings.TrimPrefix(strings.TrimSpace(msg), "FATAL")
	msg = strings.TrimSpace(msg)
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "status=451") || strings.Contains(lower, "restricted location") || strings.Contains(lower, "限制服务区域"):
		return "服务器所在地区被交易所限制访问。请更换服务器地区、配置代理，或改用当前地区可访问的交易所。"
	case strings.Contains(lower, "context deadline exceeded") || strings.Contains(lower, "client.timeout") || strings.Contains(lower, "timeout") || strings.Contains(lower, "i/o timeout"):
		return "连接交易所超时。请检查服务器网络、交易所是否可访问，以及 API 是否限制了服务器 IP。"
	case strings.Contains(lower, "no such host") || strings.Contains(lower, "temporary failure in name resolution") || strings.Contains(lower, "connection refused") || strings.Contains(lower, "network is unreachable"):
		return "服务器无法连接交易所。请检查服务器网络、DNS、防火墙或地区访问限制。"
	case strings.Contains(lower, "invalid api") || strings.Contains(lower, "api key") || strings.Contains(lower, "apikey") || strings.Contains(lower, "invalid key") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "401"):
		return "API Key 无效或权限不足。请检查 API Key、Secret Key、交易权限，并确认没有开启提现权限。"
	case strings.Contains(lower, "signature") || strings.Contains(lower, "sign") || strings.Contains(lower, "签名"):
		return "API Secret 或 Passphrase 不正确，导致签名校验失败。请重新复制 API 信息。"
	case strings.Contains(lower, "passphrase"):
		return "Passphrase 不正确或未填写。Bitget 和 OKX 必须填写创建 API 时设置的 Passphrase。"
	case strings.Contains(lower, "ip") && (strings.Contains(lower, "restrict") || strings.Contains(lower, "whitelist") || strings.Contains(lower, "not permitted") || strings.Contains(lower, "not allowed")):
		return "API 限制了 IP 白名单。请把当前服务器公网 IP 加到交易所 API 白名单。"
	case strings.Contains(lower, "insufficient") || strings.Contains(lower, "余额不足") || strings.Contains(lower, "balance") && strings.Contains(lower, "not enough"):
		return "账户可用余额不足。请充值、降低每单金额，或检查合约保证金账户是否有资金。"
	case strings.Contains(lower, "symbol") || strings.Contains(lower, "instrument") || strings.Contains(lower, "contract") || strings.Contains(lower, "instid"):
		return "交易币种不被当前交易所或当前市场支持。请检查现货/合约选择和交易对名称。"
	case strings.Contains(lower, "price") && (strings.Contains(lower, "precision") || strings.Contains(lower, "tick") || strings.Contains(lower, "invalid")):
		return "价格精度不符合交易所要求。请调整价格间隔，或换一个支持的交易对。"
	case strings.Contains(lower, "quantity") || strings.Contains(lower, "size") || strings.Contains(lower, "lot") || strings.Contains(lower, "min order"):
		return "下单数量或最小订单金额不符合交易所要求。请提高每单金额或最小订单价值。"
	case strings.Contains(lower, "websocket") || strings.Contains(lower, "web socket") || strings.Contains(lower, "价格流"):
		return "实时价格连接失败。请检查服务器能否访问交易所 WebSocket，或稍后重试。"
	case strings.Contains(lower, "无法从") && strings.Contains(lower, "获取价格"):
		return "无法读取实时价格。请检查交易对是否正确，以及服务器能否访问交易所行情接口。"
	case strings.Contains(lower, "exit status"):
		return "机器人启动后立即退出。请检查 API、交易对、余额和服务器网络。"
	}

	if msg == "" {
		return "操作失败。请检查 API、交易对、余额和服务器网络。"
	}
	if len([]rune(msg)) > 180 {
		return "操作失败。请检查 API、交易对、余额和服务器网络；详细原因可查看机器人日志。"
	}
	return msg
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *consoleServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(consoleHTML))
}
