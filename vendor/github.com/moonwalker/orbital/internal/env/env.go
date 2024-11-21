package env

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

const (
	envKey     = "ENV"
	envDefault = "dev"
)

func init() {
	// .env (default)
	godotenv.Load()

	// .env.<ENV>
	godotenv.Overload(".env." + get(envKey, envDefault))

	// .env.local # local user specific (git ignored)
	godotenv.Overload(".env.local")
}

func NatsURL() string {
	return get("NATS_URL", "nats://127.0.0.1:4222")
}

func NatsLeafURL() string {
	return get("NATS_LEAF_URL", "nats://127.0.0.1:4223")
}

func NatsLeafDomain() string {
	return get("NATS_LEAF_DOMAIN", "leaf")
}

func NatsLeafAuthEnabled() bool {
	b, _ := strconv.ParseBool(os.Getenv("NATS_LEAF_AUTH_ENABLED"))
	return b
}

func NatsLeafAuthUser() string {
	return get("NATS_LEAF_AUTH_USER", "auth")
}

func NatsLeafAuthPass() string {
	return get("NATS_LEAF_AUTH_PASS", "auth")
}

func NatsLeafAuthIssuerSeed() string {
	return get("NATS_LEAF_AUTH_SEED", "SAAGW7QOUAUCZBW47J5KF5T4QFNPP3JGW72TOSDG5C2FBYG2TWRU5JU7WI")
}

func RedisURL() string {
	return get("REDIS_URL", "redis://127.0.0.1:6379")
}

func RulesRepoBackend() string {
	return get("RULES_REPO_BACKEND", "jetstream")
}

func DomainName() string {
	return get("DOMAIN_NAME", "localhost")
}

func AuthDisplayName() string {
	return get("AUTH_NAME", "orbitals")
}

func WebAuthnAuthOrigins() []string {
	webAuthnAuthOrigins := strings.Split(os.Getenv("WEBAUTHN_AUTHORIGINS"), ",")
	return append(webAuthnAuthOrigins, "localhost")
}

func WebAuthnPassthroughDomains() []string {
	webAuthnPassthroughDomains := strings.Split(os.Getenv("WEBAUTHN_PASSTHROUGH_DOMAINS"), ",")
	return append(webAuthnPassthroughDomains, "localhost")
}

func WebAuthnPassthroughToken() string {
	return get("WEBAUTHN_PASSTHROUGH_TOKEN", "c772d3d862062645c8937d0435278688")
}

func JwtSigningKey() string {
	return get("JWT_SIGNING_KEY", "13f616ffca90cbd9003ae4dbf7db41e6e2f990a451c8a8ad66f07144cc194fb0")
}

func WebSocketJwtSigningKey() string {
	return get("WS_JWT_SIGNING_KEY", "13d9ca7d0609105c0710b217897c8c77285da7062ebdd657dc8ddd0a2953c221")
}

func PostgresSchema() string {
	return get("POSTGRES_SCHEMA", "public")
}

func PostgresMinConns() int32 {
	return int32(getint("POSTGRES_MIN_CONNS", 0))
}

func PostgresMaxConns() int32 {
	return int32(getint("POSTGRES_MAX_CONNS", 4))
}

func RequestLogger() bool {
	b, _ := strconv.ParseBool(os.Getenv("REQUEST_LOGGER"))
	return b
}

func OtelCollectorEndpoint() string {
	return os.Getenv("OTEL_COLLECTOR_ENDPOINT")
}

// private functions

func get(key string, def string) string {
	s := os.Getenv(key)
	if len(s) == 0 {
		return def // return default
	}
	return s
}

func getint(key string, def int) int {
	i, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return def
	}
	return i
}
