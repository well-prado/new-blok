export NO_PROXY="$(echo "$NO_PROXY" | sed 's/,proxy\.golang\.org//')"
export no_proxy="$NO_PROXY"
export GOTOOLCHAIN=auto
# go1.27.1 matches the laptop and the internal/package fixture (which runs go with GOTOOLCHAIN=local).
export PATH=/root/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.1.linux-amd64/bin:$PATH
