package cache

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/meltwater/drone-cache/archive"
	"github.com/meltwater/drone-cache/key/generator"
	"github.com/meltwater/drone-cache/storage"
	"github.com/meltwater/drone-cache/storage/backend"
	"github.com/meltwater/drone-cache/storage/backend/filesystem"
	"github.com/meltwater/drone-cache/test"
)

func TestRestore(t *testing.T) {
	// Implement me!
	t.Skip("skipping unimplemented test.")
}

func TestRestoreIntoWorkspaceOtherThanTheRebuilds(t *testing.T) {
	base := t.TempDir()
	built := filepath.Join(base, "concierge_39")
	restoring := filepath.Join(base, "concierge_40")
	test.Ok(t, os.MkdirAll(filepath.Join(built, "node_modules", ".bin"), 0755))
	test.Ok(t, os.MkdirAll(restoring, 0755))
	test.Ok(t, os.MkdirAll(filepath.Join(base, "remote"), 0755))
	test.Ok(t, os.WriteFile(filepath.Join(built, "node_modules", "mocha.js"), []byte("mocha\n"), 0644))
	test.Ok(t, os.Symlink("../mocha.js", filepath.Join(built, "node_modules", ".bin", "_mocha")))

	logger := log.NewNopLogger()
	b, err := backend.FromConfig(logger, backend.FileSystem,
		backend.Config{FileSystem: filesystem.Config{CacheRoot: filepath.Join(base, "remote")}})
	test.Ok(t, err)
	s := storage.New(logger, b, time.Minute)
	inWorkspace := func(workspace string) Cache {
		t.Chdir(workspace)
		return New(logger, s, archive.FromFormat(logger, workspace, archive.Tar), generator.NewStatic("key"))
	}

	test.Ok(t, inWorkspace(built).Rebuild([]string{"node_modules"}, false))
	test.Ok(t, os.RemoveAll(filepath.Join(built, "node_modules")))
	test.Ok(t, inWorkspace(restoring).Restore([]string{"node_modules"}, false))

	got, err := os.ReadFile(filepath.Join(restoring, "node_modules", ".bin", "_mocha"))
	test.Ok(t, err)
	test.Equals(t, string(got), "mocha\n")
	_, err = os.Lstat(filepath.Join(built, "node_modules"))
	test.Assert(t, os.IsNotExist(err), "cache restored into the workspace it was rebuilt in: %v", err)
}
