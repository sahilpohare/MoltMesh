package p2putil

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// AddrsToAddrInfo parses a list of multiaddr strings and returns a peer.AddrInfo.
// All addrs must embed the same peer ID (p2p component).
func AddrsToAddrInfo(addrs []string) (*peer.AddrInfo, error) {
	var maddrs []multiaddr.Multiaddr
	for _, a := range addrs {
		ma, err := multiaddr.NewMultiaddr(a)
		if err != nil {
			continue // skip malformed
		}
		maddrs = append(maddrs, ma)
	}
	if len(maddrs) == 0 {
		return nil, fmt.Errorf("no valid multiaddrs")
	}
	// peer.AddrInfosFromP2pAddrs deduplicates peer ID and collects all addrs
	infos, err := peer.AddrInfosFromP2pAddrs(maddrs...)
	if err != nil {
		return nil, err
	}
	if len(infos) == 0 {
		return nil, fmt.Errorf("could not extract peer info")
	}
	return &infos[0], nil
}
