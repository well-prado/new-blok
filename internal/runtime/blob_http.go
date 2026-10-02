package runtime

import (
	contract "github.com/well-prado/new-blok/contract/runtime"
	"net/http"
	"strconv"
	"strings"
)

// BlobHandler is selected by application composition on an authenticated local
// listener. It exposes only reads from the credential's own bounded namespace.
// Applications own server connection limits/timeouts; no listener starts here.
func BlobHandler(auth *TokenAuthenticator, store *BlobStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth == nil || store == nil || r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/blobs/") {
			http.Error(w, "unavailable", 404)
			return
		}
		if len(r.Header.Values("Authorization")) != 1 || len(r.Header.Values("X-Blok-Principal")) != 1 {
			http.Error(w, "denied", 401)
			return
		}
		authorization := r.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") {
			http.Error(w, "denied", 401)
			return
		}
		session, err := auth.NewSession(strings.TrimPrefix(authorization, "Bearer "), r.Header.Get("X-Blok-Principal"), []contract.Capability{"blob:read"})
		if err != nil {
			http.Error(w, "denied", 401)
			return
		}
		size, err := strconv.Atoi(r.URL.Query().Get("size"))
		if err != nil || size < 0 || size > contract.MaxBlobBytes {
			http.Error(w, "invalid reference", 400)
			return
		}
		ref := contract.BlobRef{Digest: strings.TrimPrefix(r.URL.Path, "/blobs/"), Size: size}
		data, err := store.GetAuthorized(session, ref)
		if err != nil {
			http.Error(w, "unavailable", 404)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(data)
	})
}
