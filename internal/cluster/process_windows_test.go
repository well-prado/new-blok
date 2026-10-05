//go:build windows

package cluster

import (
	"errors"
	"os"
)

func suspendProcess(*os.Process) error { return errors.ErrUnsupported }

func resumeProcess(*os.Process) error { return errors.ErrUnsupported }
