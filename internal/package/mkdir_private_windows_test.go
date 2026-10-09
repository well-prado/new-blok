//go:build windows

package packagemanager

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// permissiveDir creates a directory whose inheritable DACL lets every
// authenticated user modify it, like a volume root or C:\ProgramData.
func permissiveDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "shared volume")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;OW)(A;OICI;0x1301bf;;;AU)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatal(err)
	}
	if !grantsTo(t, dir, "S-1-5-11") {
		t.Fatal("fixture is not permissive; the test proves nothing")
	}
	return dir
}

// grantsTo reports whether path's DACL has an allow entry for sid.
func grantsTo(t *testing.T, path, sid string) bool {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			t.Fatal(err)
		}
		if ace.Header.AceType == windows.ACCESS_ALLOWED_ACE_TYPE && (*windows.SID)(unsafe.Pointer(&ace.SidStart)).String() == sid {
			return true
		}
	}
	return false
}

// #156: Windows ignores 0700, so a cache outside the user profile inherited
// the volume's ACL and every authenticated user could modify packages.
func TestCacheDirectoriesAreOwnerOnlyOnWindows(t *testing.T) {
	shared := permissiveDir(t)
	root := filepath.Join(shared, "nested", "cache")
	if _, err := NewCache(root); err != nil {
		t.Fatal(err)
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{filepath.Join(shared, "nested"), root, filepath.Join(root, "blobs"), filepath.Join(root, "indexes")} {
		for _, other := range []string{"S-1-5-11", "S-1-5-32-545", "S-1-1-0"} { // Authenticated Users, Users, Everyone
			if grantsTo(t, dir, other) {
				t.Fatalf("%s grants access to %s", dir, other)
			}
		}
		if !grantsTo(t, dir, user.User.Sid.String()) {
			t.Fatalf("%s does not grant its owner access", dir)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "blobs", "probe"), []byte("x"), 0o600); err != nil {
		t.Fatalf("owner cannot write into the cache: %v", err)
	}
	if grantsTo(t, filepath.Join(root, "blobs", "probe"), "S-1-5-11") {
		t.Fatal("a file in the cache inherited access for other users")
	}
}

// Like os.MkdirAll with POSIX modes, an existing directory is left alone.
func TestExistingCacheDirectoryKeepsItsACL(t *testing.T) {
	shared := permissiveDir(t)
	if _, err := NewCache(shared); err != nil {
		t.Fatal(err)
	}
	if !grantsTo(t, shared, "S-1-5-11") {
		t.Fatal("NewCache rewrote the ACL of a directory it did not create")
	}
}
