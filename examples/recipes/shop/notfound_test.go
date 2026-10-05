package shop

import (
	"io"
	"net/http"
	"reflect"
	"regexp"
	"testing"
)

var recipeRequestID = regexp.MustCompile(`"requestId":"[^"]*"`)

// TestHiddenAndMissingRecordsAnswerIdentical404 is #306's acceptance over the
// recipe's real HTTP listener: Bob reading, updating or deleting Alice's
// record gets 404 with exactly the answer he gets for a record that does
// not exist. Status, body (apart from the request id) and headers (apart
// from Date, and Content-Length, which follows the request id's length) are
// identical, and Alice's record is untouched.
func TestHiddenAndMissingRecordsAnswerIdentical404(t *testing.T) {
	application, server := startedRecipe(t, "hidden.db")
	created := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"hidden-1","value":"alice-private"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.StatusCode, responseBody(t, created))
	}
	_ = created.Body.Close()
	beforeRecords, beforeOutbox := recipeCounts(t, application)
	type answer struct {
		status int
		header http.Header
		body   string
	}
	call := func(method, id, body string) answer {
		t.Helper()
		response := request(t, server.Client(), method, server.URL+"/records/"+id, bobToken, body)
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		header := response.Header.Clone()
		header.Del("Date")
		header.Del("Content-Length")
		return answer{status: response.StatusCode, header: header, body: recipeRequestID.ReplaceAllString(string(data), `"requestId":"-"`)}
	}
	for _, tc := range []struct {
		name, method, hiddenBody, missingBody string
	}{
		{"read", http.MethodGet, "", ""},
		{"update", http.MethodPut, `{"requestKey":"bob-update-hidden","value":"stolen"}`, `{"requestKey":"bob-update-missing","value":"stolen"}`},
		{"delete", http.MethodDelete, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hidden, missing := call(tc.method, "hidden-1", tc.hiddenBody), call(tc.method, "never-existed", tc.missingBody)
			want := fixture(t, "cross-principal-"+tc.name).ExpectedStatus
			if hidden.status != want || hidden.body != `{"error":"not_found","requestId":"-"}`+"\n" {
				t.Fatalf("another principal's record: %d %s; want %d not_found", hidden.status, hidden.body, want)
			}
			if !reflect.DeepEqual(hidden, missing) {
				t.Fatalf("distinguishable:\nhidden  %+v\nmissing %+v", hidden, missing)
			}
		})
	}
	if records, outbox := recipeCounts(t, application); records != beforeRecords || outbox != beforeOutbox {
		t.Fatalf("hidden-record calls changed records/outbox %d/%d to %d/%d", beforeRecords, beforeOutbox, records, outbox)
	}
	read := request(t, server.Client(), http.MethodGet, server.URL+"/records/hidden-1", aliceToken, "")
	if body := responseBody(t, read); read.StatusCode != http.StatusOK || !regexp.MustCompile(`"alice-private"`).MatchString(body) {
		t.Fatalf("owner read after hidden calls: %d %s", read.StatusCode, body)
	}
}
