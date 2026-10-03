package sse_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

// frame is one parsed Server-Sent Events frame, or a comment.
type frame struct {
	ID      string
	Type    string
	Data    string
	Retry   string
	Comment string
}

// eventStream reads an event stream exactly as the wire carries it.
type eventStream struct {
	response *http.Response
	reader   *bufio.Reader
	cancel   context.CancelFunc
	frames   chan frame
	err      chan error
}

// subscribe opens a subscription; a non-200 response is returned with its
// body read and closed.
func subscribe(ctx context.Context, client *http.Client, url, credential, cursor string) (*eventStream, *http.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	request.Header.Set("Accept", "text/event-stream")
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	if cursor != "" {
		request.Header.Set("Last-Event-ID", cursor)
	}
	response, err := client.Do(request)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		response.Body = io.NopCloser(strings.NewReader(string(body)))
		return nil, response, nil
	}
	s := &eventStream{response: response, reader: bufio.NewReader(response.Body), cancel: cancel, frames: make(chan frame, 1024), err: make(chan error, 1)}
	go s.read()
	return s, nil, nil
}

func (s *eventStream) read() {
	var current frame
	dataLines := 0
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			s.err <- err
			close(s.frames)
			return
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			if current != (frame{}) || dataLines > 0 {
				s.frames <- current
			}
			current, dataLines = frame{}, 0
		case strings.HasPrefix(line, ":"):
			s.frames <- frame{Comment: strings.TrimPrefix(line, ": ")}
		default:
			field, value, _ := strings.Cut(line, ": ")
			switch field {
			case "id":
				current.ID = value
			case "event":
				current.Type = value
			case "retry":
				current.Retry = value
			case "data":
				if dataLines > 0 {
					current.Data += "\n"
				}
				current.Data += value
				dataLines++
			}
		}
	}
}

// next returns the next frame, skipping comments unless asked for them.
func (s *eventStream) next(timeout time.Duration, comments bool) (frame, error) {
	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-s.frames:
			if !ok {
				return frame{}, io.EOF
			}
			if f.Comment != "" && !comments {
				continue
			}
			return f, nil
		case <-deadline:
			return frame{}, errors.New("timed out waiting for a frame")
		}
	}
}

// ended waits for the server to close the stream.
func (s *eventStream) ended(timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case _, ok := <-s.frames:
			if !ok {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

func (s *eventStream) close() {
	s.cancel()
	s.response.Body.Close()
}
