package generate

import (
	"os"
	"path/filepath"
)

// WriteFile replaces path with data atomically: the bytes go to a temporary
// file in the same directory, which is renamed over path, so a reader (a
// compiler, or blok dev's watcher) never sees a half-written file. blok
// generate and blok dev's regeneration share it.
func WriteFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".blok-generate-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, path)
}
