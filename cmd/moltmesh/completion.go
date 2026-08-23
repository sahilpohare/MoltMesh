package main

import (
	"fmt"
	"os"
	"strings"
)

// allCommands lists every top-level command, generated from cliCommands
// (main.go) so it can't drift from the actual dispatch table the way the
// old hand-maintained copy did. A function, not a package-level var: it's
// referenced from within cliCommands (as cmdCompletion), so a var initializer
// here would create an initialization cycle.
func allCommands() []string {
	cmds := cliCommands()
	names := make([]string, len(cmds))
	for i, c := range cmds {
		names[i] = c.name
	}
	return names
}

func cmdCompletion(args []string) error {
	shell := "bash"
	if len(args) > 0 {
		shell = args[0]
	}
	commands := strings.Join(allCommands(), " ")
	switch shell {
	case "bash":
		fmt.Print(strings.Replace(bashCompletion, "{{COMMANDS}}", commands, 1))
	case "zsh":
		fmt.Print(zshCompletion)
	case "fish":
		fmt.Print(strings.Replace(fishCompletion, "{{COMMANDS}}", commands, 1))
	default:
		fmt.Fprintf(os.Stderr, "unknown shell %q; valid: bash, zsh, fish\n", shell)
		return fmt.Errorf("unknown shell")
	}
	return nil
}

const bashCompletion = `# moltmesh bash completion
# Add to ~/.bashrc:  source <(moltmesh completion bash)

_moltmesh_complete() {
    local cur prev words cword
    _init_completion || return

    local commands="{{COMMANDS}}"

    local global_flags="--data-dir --grpc-addr --json"

    if [[ $cword -eq 1 ]]; then
        COMPREPLY=($(compgen -W "$commands" -- "$cur"))
        return
    fi

    case "${words[1]}" in
        network)
            COMPREPLY=($(compgen -W "create join leave list members broadcast subscribe" -- "$cur"))
            ;;
        name)
            COMPREPLY=($(compgen -W "claim resolve" -- "$cur"))
            ;;
        format)
            COMPREPLY=($(compgen -W "did capability multiaddr bytes time" -- "$cur"))
            ;;
        completion)
            COMPREPLY=($(compgen -W "bash zsh fish" -- "$cur"))
            ;;
        update-task)
            case "$prev" in
                --status) COMPREPLY=($(compgen -W "working completed failed cancelled" -- "$cur")) ; return ;;
            esac
            COMPREPLY=($(compgen -W "--id --status --error $global_flags" -- "$cur"))
            ;;
        send-message)    COMPREPLY=($(compgen -W "--to --text --thread-id $global_flags" -- "$cur")) ;;
        get-inbox)       COMPREPLY=($(compgen -W "--limit --unread --thread-id --task-id $global_flags" -- "$cur")) ;;
        get-outbox)      COMPREPLY=($(compgen -W "--status --limit $global_flags" -- "$cur")) ;;
        get-agent-card)  COMPREPLY=($(compgen -W "--did $global_flags" -- "$cur")) ;;
        publish-agent-card) COMPREPLY=($(compgen -W "--name --description $global_flags" -- "$cur")) ;;
        find-agents)     COMPREPLY=($(compgen -W "--capability --limit $global_flags" -- "$cur")) ;;
        create-task)     COMPREPLY=($(compgen -W "--to --skill --thread-id $global_flags" -- "$cur")) ;;
        get-task|cancel-task) COMPREPLY=($(compgen -W "--id $global_flags" -- "$cur")) ;;
        publish-task-event) COMPREPLY=($(compgen -W "--task-id --kind --data $global_flags" -- "$cur")) ;;
        subscribe-task-events) COMPREPLY=($(compgen -W "--id $global_flags" -- "$cur")) ;;
        send-file)       COMPREPLY=($(compgen -W "--file --mime-type $global_flags" -- "$cur")) ;;
        fetch-file)      COMPREPLY=($(compgen -W "--cid --from --out $global_flags" -- "$cur")) ;;
        create-thread)   COMPREPLY=($(compgen -W "--replicas --f --epoch-ms $global_flags" -- "$cur")) ;;
        get-thread|subscribe-thread) COMPREPLY=($(compgen -W "--id $global_flags" -- "$cur")) ;;
        append-entry)    COMPREPLY=($(compgen -W "--thread-id --payload --kind $global_flags" -- "$cur")) ;;
        get-thread-entries) COMPREPLY=($(compgen -W "--id --since --limit $global_flags" -- "$cur")) ;;
        ping)            COMPREPLY=($(compgen -W "--count $global_flags" -- "$cur")) ;;
        publish)         COMPREPLY=($(compgen -W "--topic --payload $global_flags" -- "$cur")) ;;
        subscribe-topic) COMPREPLY=($(compgen -W "--topic $global_flags" -- "$cur")) ;;
        set-webhook)     COMPREPLY=($(compgen -W "--url --secret $global_flags" -- "$cur")) ;;
        start)           COMPREPLY=($(compgen -W "--data-dir --port --grpc-addr --verbose --config" -- "$cur")) ;;
        *)               COMPREPLY=($(compgen -W "$global_flags" -- "$cur")) ;;
    esac
}

complete -F _moltmesh_complete moltmesh
`

const zshCompletion = `#compdef moltmesh
# moltmesh zsh completion
# Add to ~/.zshrc:  source <(moltmesh completion zsh)

_moltmesh() {
    local state

    _arguments \
        '--data-dir[Data directory]:dir:_files -/' \
        '--grpc-addr[gRPC address]:addr:' \
        '--json[JSON output]' \
        '1:command:->command' \
        '*:args:->args'

    case $state in
        command)
            # Descriptions make this list unrenderable from allCommands (a
            # plain name list) — keep it in sync with cliCommands in main.go
            # by hand; bash/fish derive their name-only lists from
            # allCommands directly and can't drift.
            local commands=(
                'start:Start daemon in background'
                'init:Scaffold a new agent'
                'stop:Stop running daemon'
                'status:Check daemon status'
                'info:Show node info'
                'tui:Open interactive TUI'
                'version:Show version'
                'help:Show help'
                'identity:Show identity'
                'config:Show config paths'
                'get-identity:Get node identity'
                'get-agent-card:Get agent card for a DID'
                'publish-agent-card:Publish agent card'
                'find-agents:Find agents by capability'
                'send-message:Send a message'
                'get-inbox:List inbox messages'
                'get-outbox:List outbox messages'
                'subscribe-inbox:Stream incoming messages'
                'ack-message:Acknowledge a message'
                'create-task:Create a task'
                'send-task-result:Send a terminal result to a task initiator'
                'get-task:Get task by ID'
                'update-task:Update task status'
                'cancel-task:Cancel a task'
                'publish-task-event:Publish a task event'
                'subscribe-task-events:Stream task events'
                'send-file:Upload a file'
                'fetch-file:Download a file'
                'create-thread:Create a thread'
                'get-thread:Get thread info'
                'append-entry:Append thread entry'
                'get-thread-entries:List thread entries'
                'subscribe-thread:Stream thread entries'
                'add-thread-replica:Add an observer DID as a replica'
                'recover-thread:Recover verified history'
                'ping:Ping a peer'
                'health:Show daemon health'
                'peers:List connected peers'
                'publish:Publish to a topic'
                'subscribe-topic:Stream topic messages'
                'set-webhook:Set webhook URL'
                'clear-webhook:Remove webhook'
                'get-webhook:Show webhook URL'
                'network:Network subcommands'
                'name:Name subcommands'
                'format:Format utilities'
                'completion:Generate shell completion'
            )
            _describe 'command' commands
            ;;
        args)
            case $words[2] in
                network)
                    local subs=('create' 'join' 'leave' 'list' 'members' 'broadcast' 'subscribe')
                    _describe 'subcommand' subs
                    ;;
                name)
                    local subs=('claim' 'resolve')
                    _describe 'subcommand' subs
                    ;;
                format)
                    local types=('did' 'capability' 'multiaddr' 'bytes' 'time')
                    _describe 'type' types
                    ;;
                completion)
                    local shells=('bash' 'zsh' 'fish')
                    _describe 'shell' shells
                    ;;
                send-message)
                    _arguments '--to[Recipient DID]:did:' '--text[Message text]:text:' '--thread-id[Thread ID]:id:'
                    ;;
                update-task)
                    _arguments '--id[Task ID]:id:' '--status[Status]:(working completed failed cancelled)' '--error[Error message]:msg:'
                    ;;
                send-file)
                    _arguments '--file[File path]:file:_files' '--mime-type[MIME type]:mime:'
                    ;;
                fetch-file)
                    _arguments '--cid[Content ID]:cid:' '--from[Source DID]:did:' '--out[Output path]:file:_files'
                    ;;
                start)
                    _arguments '--data-dir[Data dir]:dir:_files -/' '--port[Port]:port:' '--verbose[Verbose]' '--config[Config file]:file:_files'
                    ;;
            esac
            ;;
    esac
}

_moltmesh
`

const fishCompletion = `# moltmesh fish completion
# Add to ~/.config/fish/completions/moltmesh.fish  or:
#   moltmesh completion fish > ~/.config/fish/completions/moltmesh.fish

set -l commands {{COMMANDS}}

# Top-level commands
complete -c moltmesh -f -n '__fish_use_subcommand' -a "$commands"

# Global flags
complete -c moltmesh -l data-dir  -d 'Data directory'    -r
complete -c moltmesh -l grpc-addr -d 'gRPC address'      -r
complete -c moltmesh -l json      -d 'JSON output'

# network subcommands
complete -c moltmesh -f -n '__fish_seen_subcommand_from network' -a 'create join leave list members broadcast subscribe'

# name subcommands
complete -c moltmesh -f -n '__fish_seen_subcommand_from name' -a 'claim resolve'

# format types
complete -c moltmesh -f -n '__fish_seen_subcommand_from format' -a 'did capability multiaddr bytes time'

# completion shells
complete -c moltmesh -f -n '__fish_seen_subcommand_from completion' -a 'bash zsh fish'

# send-message
complete -c moltmesh -n '__fish_seen_subcommand_from send-message' -l to       -d 'Recipient DID' -r
complete -c moltmesh -n '__fish_seen_subcommand_from send-message' -l text     -d 'Message text'  -r
complete -c moltmesh -n '__fish_seen_subcommand_from send-message' -l thread-id -d 'Thread ID'    -r

# get-inbox
complete -c moltmesh -n '__fish_seen_subcommand_from get-inbox' -l limit     -d 'Max messages' -r
complete -c moltmesh -n '__fish_seen_subcommand_from get-inbox' -l unread    -d 'Unread only'
complete -c moltmesh -n '__fish_seen_subcommand_from get-inbox' -l thread-id -d 'Thread ID'    -r
complete -c moltmesh -n '__fish_seen_subcommand_from get-inbox' -l task-id   -d 'Task ID'      -r

# update-task
complete -c moltmesh -n '__fish_seen_subcommand_from update-task' -l id     -d 'Task ID' -r
complete -c moltmesh -n '__fish_seen_subcommand_from update-task' -l status -d 'Status'  -r -a 'working completed failed cancelled'
complete -c moltmesh -n '__fish_seen_subcommand_from update-task' -l error  -d 'Error'   -r

# send-file / fetch-file
complete -c moltmesh -n '__fish_seen_subcommand_from send-file'  -l file      -d 'File path' -r -F
complete -c moltmesh -n '__fish_seen_subcommand_from send-file'  -l mime-type -d 'MIME type' -r
complete -c moltmesh -n '__fish_seen_subcommand_from fetch-file' -l cid       -d 'CID'       -r
complete -c moltmesh -n '__fish_seen_subcommand_from fetch-file' -l from      -d 'Source DID' -r
complete -c moltmesh -n '__fish_seen_subcommand_from fetch-file' -l out       -d 'Output path' -r -F

# start
complete -c moltmesh -n '__fish_seen_subcommand_from start' -l port    -d 'Port'        -r
complete -c moltmesh -n '__fish_seen_subcommand_from start' -l verbose -d 'Verbose'
complete -c moltmesh -n '__fish_seen_subcommand_from start' -l config  -d 'Config file' -r -F
`
