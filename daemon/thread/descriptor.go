package thread

import (
	"encoding/base64"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const descriptorSignatureKey = "creator_signature"

func descriptorBytes(th *pb.Thread) ([]byte, error) {
	clone := proto.Clone(th).(*pb.Thread)
	delete(clone.Metadata, descriptorSignatureKey)
	return proto.MarshalOptions{Deterministic: true}.Marshal(clone)
}

func SignDescriptor(th *pb.Thread, id *identity.Identity) error {
	if th.CreatorDid != id.DID {
		return fmt.Errorf("only creator can sign thread descriptor")
	}
	if th.Metadata == nil {
		th.Metadata = map[string]string{}
	}
	data, err := descriptorBytes(th)
	if err != nil {
		return err
	}
	th.Metadata[descriptorSignatureKey] = base64.StdEncoding.EncodeToString(id.Sign(data))
	return nil
}

func VerifyDescriptor(th *pb.Thread) error {
	if th == nil || th.Id == "" || th.CreatorDid == "" {
		return fmt.Errorf("invalid thread descriptor")
	}
	encoded := th.Metadata[descriptorSignatureKey]
	sig, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decode creator signature: %w", err)
	}
	data, err := descriptorBytes(th)
	if err != nil {
		return err
	}
	ok, err := identity.VerifyDID(th.CreatorDid, data, sig)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("invalid creator signature")
	}
	return nil
}
