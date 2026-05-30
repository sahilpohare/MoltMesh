// Package capability provides utilities for the a2a capability ID namespace.
//
// Canonical format:  a2a:v<version>:cap:<name>
// Examples:
//   a2a:v1:cap:text-generation
//   a2a:v1:cap:code-execution
//   a2a:v1:cap:image-analysis
package capability

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	Scheme  = "a2a"
	Version = "v1"

	// Well-known capability names.
	TextGeneration  = "a2a:v1:cap:text-generation"
	CodeExecution   = "a2a:v1:cap:code-execution"
	ImageAnalysis   = "a2a:v1:cap:image-analysis"
	FileProcessing  = "a2a:v1:cap:file-processing"
	DataRetrieval   = "a2a:v1:cap:data-retrieval"
	TaskOrchestrate = "a2a:v1:cap:task-orchestration"
	VoiceSynthesis  = "a2a:v1:cap:voice-synthesis"
	Search          = "a2a:v1:cap:search"
)

var validName = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

// New builds a canonical capability ID: "a2a:v1:cap:<name>".
// Returns an error if name contains invalid characters.
func New(name string) (string, error) {
	if err := validateName(name); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s:cap:%s", Scheme, Version, name), nil
}

// MustNew panics if name is invalid. Use only in package-level var declarations.
func MustNew(name string) string {
	s, err := New(name)
	if err != nil {
		panic(err)
	}
	return s
}

// Validate returns an error if capID is not a well-formed capability ID.
func Validate(capID string) error {
	parts := strings.SplitN(capID, ":", 4)
	if len(parts) != 4 {
		return fmt.Errorf("capability ID must have 4 colon-separated parts: %q", capID)
	}
	if parts[0] != Scheme {
		return fmt.Errorf("capability scheme must be %q, got %q", Scheme, parts[0])
	}
	if !strings.HasPrefix(parts[1], "v") {
		return fmt.Errorf("capability version must start with 'v', got %q", parts[1])
	}
	if parts[2] != "cap" {
		return fmt.Errorf("capability type must be \"cap\", got %q", parts[2])
	}
	return validateName(parts[3])
}

// IsValid reports whether capID is a valid capability ID.
func IsValid(capID string) bool { return Validate(capID) == nil }

// Parse breaks a capability ID into its components.
// Returns scheme, version, name and an error if malformed.
func Parse(capID string) (scheme, version, name string, err error) {
	if err = Validate(capID); err != nil {
		return
	}
	parts := strings.SplitN(capID, ":", 4)
	return parts[0], parts[1], parts[3], nil
}

// Name returns just the name segment (e.g. "text-generation") from a full capability ID.
// Returns the original string if not parseable.
func Name(capID string) string {
	_, _, name, err := Parse(capID)
	if err != nil {
		return capID
	}
	return name
}

// Short returns a display-friendly form. For well-known IDs the name is sufficient;
// for unknown ones the full string is returned.
func Short(capID string) string {
	name := Name(capID)
	if name == capID {
		return capID // not parseable
	}
	return name
}

// WithVersion returns the same capability ID but with a different version.
func WithVersion(capID, ver string) (string, error) {
	_, _, name, err := Parse(capID)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s:cap:%s", Scheme, ver, name), nil
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("capability name must not be empty")
	}
	if !validName.MatchString(name) {
		return fmt.Errorf("capability name must be lowercase alphanumeric with hyphens: %q", name)
	}
	return nil
}
