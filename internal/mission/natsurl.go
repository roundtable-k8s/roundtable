package mission

import (
	"os"
	"strings"
)

// EnvDefaultNATSURL overrides the NATS URL used for missions whose parent
// RoundTable and roundTableTemplate set none (chart: missionDefaultNatsURL).
const EnvDefaultNATSURL = "MISSION_DEFAULT_NATS_URL"

const builtinDefaultNATSURL = "nats://nats.database.svc.cluster.local:4222"

// DefaultMissionNATSURL returns the fallback mission NATS URL from the
// environment, or the built-in default when unset.
func DefaultMissionNATSURL() string {
	if v := strings.TrimSpace(os.Getenv(EnvDefaultNATSURL)); v != "" {
		return v
	}
	return builtinDefaultNATSURL
}
