package nats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// runCredsServer starts an in-process server that rejects anonymous clients
// and returns it with the path of a valid user credentials file.
func runCredsServer(t *testing.T) (*natsserver.Server, string) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	op, _ := nkeys.CreateOperator()
	opPub, _ := op.PublicKey()
	acc, _ := nkeys.CreateAccount()
	accPub, _ := acc.PublicKey()
	usr, _ := nkeys.CreateUser()
	usrPub, _ := usr.PublicKey()
	usrSeed, _ := usr.Seed()

	opJWT, err := jwt.NewOperatorClaims(opPub).Encode(op)
	must(err)
	accJWT, err := jwt.NewAccountClaims(accPub).Encode(op)
	must(err)
	usrJWT, err := jwt.NewUserClaims(usrPub).Encode(acc)
	must(err)
	creds, err := jwt.FormatUserConfig(usrJWT, usrSeed)
	must(err)
	path := filepath.Join(t.TempDir(), "user.creds")
	must(os.WriteFile(path, creds, 0o600))

	opClaims, err := jwt.DecodeOperatorClaims(opJWT)
	must(err)
	resolver := &natsserver.MemAccResolver{}
	must(resolver.Store(accPub, accJWT))
	s, err := natsserver.NewServer(&natsserver.Options{
		Port:             -1,
		TrustedOperators: []*jwt.OperatorClaims{opClaims},
		AccountResolver:  resolver,
	})
	must(err)
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s, path
}

func TestConnectWithCredsFile(t *testing.T) {
	s, creds := runCredsServer(t)

	withCreds := NewClient(Config{URL: s.ClientURL(), CredsFile: creds}, logr.Discard())
	if err := withCreds.Connect(); err != nil {
		t.Fatalf("connect with creds: %v", err)
	}
	if !withCreds.IsConnected() {
		t.Fatal("not connected")
	}
	_ = withCreds.Close()

	anon := NewClient(Config{URL: s.ClientURL()}, logr.Discard())
	if err := anon.Connect(); err == nil {
		_ = anon.Close()
		t.Fatal("anonymous connection to a credentials-only server was accepted")
	}
}

func TestCredsFileMissingNamesPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.creds")
	c := NewClient(Config{URL: "nats://127.0.0.1:1", CredsFile: path}, logr.Discard())
	err := c.Connect()
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("err = %v, want one naming %s", err, path)
	}
}

func TestCredsOptionEmptyIsNil(t *testing.T) {
	opt, err := CredsOption("")
	if err != nil || opt != nil {
		t.Fatalf("CredsOption(\"\") = %v, %v; want nil, nil", opt, err)
	}
	if DefaultConfig().CredsFile != "" {
		t.Fatal("default config carries a credentials file")
	}
}
