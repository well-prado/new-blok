//go:build windows

package packagemanager

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// mkdirPrivate creates dir and any missing parents as owner-only, the
// Windows counterpart of POSIX 0700, which Windows ignores apart from the
// read-only bit. Without it a cache outside the user profile (D:\cache,
// C:\ProgramData\...) inherits the volume's ACL, where every authenticated
// user may modify it. The first directory created gets a protected DACL
// granting only the current user, SYSTEM and Administrators, inherited by
// everything created beneath it. Existing directories are left as they are,
// as os.MkdirAll leaves their POSIX modes.
func mkdirPrivate(dir string) error {
	dir = filepath.Clean(dir)
	missing := dir
	for {
		parent := filepath.Dir(missing)
		if _, err := os.Stat(parent); err == nil || parent == missing {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		missing = parent
	}
	if _, err := os.Stat(missing); err == nil {
		return nil // dir already exists
	}
	if err := os.Mkdir(missing, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	} else if err == nil {
		if err := restrictToOwner(missing); err != nil {
			_ = os.Remove(missing)
			return err
		}
	}
	return os.MkdirAll(dir, 0o700)
}

func restrictToOwner(path string) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	descriptor, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;" + user.User.Sid.String() + ")(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)")
	if err != nil {
		return err
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil)
}
