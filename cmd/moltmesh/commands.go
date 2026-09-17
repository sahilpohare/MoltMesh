package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// jsonMode is set by the global --json flag.
var jsonMode bool

// jsonOut prints data as a JSON envelope {"status":"ok","data":...} when --json is set,
// otherwise marshals the value with indentation.

func jsonOut(data interface{}) {
	if jsonMode {
		b, _ := json.Marshal(map[string]interface{}{"status": "ok", "data": data})
		fmt.Println(string(b))
	} else {
		b, _ := json.MarshalIndent(data, "", "  ")
		fmt.Println(string(b))
	}
}

// jsonErr prints a structured error when --json is set, otherwise prints plain text.
func jsonErr(code, msg string) {
	if jsonMode {
		b, _ := json.Marshal(map[string]string{"status": "error", "code": code, "message": msg})
		fmt.Fprintln(os.Stderr, string(b))
	} else {
		fmt.Fprintln(os.Stderr, "error:", msg)
	}
}

// ── Identity & Registry ───────────────────────────────────────────────────────
