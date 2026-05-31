package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/lipgloss"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// ─── Styles ───────────────────────────────────────────────────────────────────

var (
	colorPrimary  = lipgloss.Color("#7C3AED")
	colorAccent   = lipgloss.Color("#A78BFA")
	colorMuted    = lipgloss.Color("#6B7280")
	colorSuccess  = lipgloss.Color("#10B981")
	colorWarning  = lipgloss.Color("#F59E0B")
	colorDanger   = lipgloss.Color("#EF4444")
	colorText     = lipgloss.Color("#F3F4F6")
	colorBg       = lipgloss.Color("#111827")
	colorBorder   = lipgloss.Color("#374151")
	colorSelected = lipgloss.Color("#1F2937")

	styleTab = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(colorMuted)

	styleTabActive = lipgloss.NewStyle().
			Padding(0, 2).
			Foreground(colorAccent).
			Bold(true).
			BorderBottom(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(colorPrimary)

	styleTabBar = lipgloss.NewStyle().
			BorderBottom(true).
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(colorBorder)

	stylePanel = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(0, 1)

	stylePanelActive = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorPrimary).
				Padding(0, 1)

	styleTitle = lipgloss.NewStyle().
			Foreground(colorAccent).
			Bold(true).
			MarginBottom(1)

	styleLabel = lipgloss.NewStyle().
			Foreground(colorMuted).
			Width(12)

	styleValue = lipgloss.NewStyle().
			Foreground(colorText)

	styleDID = lipgloss.NewStyle().
			Foreground(colorAccent).
			Italic(true)

	styleStatusBar = lipgloss.NewStyle().
			Background(colorBg).
			Foreground(colorMuted).
			Padding(0, 1)

	styleSuccess = lipgloss.NewStyle().Foreground(colorSuccess)
	styleWarning = lipgloss.NewStyle().Foreground(colorWarning)
	styleDanger  = lipgloss.NewStyle().Foreground(colorDanger)
	styleMuted   = lipgloss.NewStyle().Foreground(colorMuted)

	styleInput = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorPrimary).
			Padding(0, 1)

	styleButton = lipgloss.NewStyle().
			Background(colorPrimary).
			Foreground(colorText).
			Padding(0, 2).
			Bold(true)

	styleButtonMuted = lipgloss.NewStyle().
				Background(colorBorder).
				Foreground(colorMuted).
				Padding(0, 2)
)

// ─── Tabs ─────────────────────────────────────────────────────────────────────

type tab int

const (
	tabIdentity tab = iota
	tabInbox
	tabCompose
	tabTasks
	tabPeers
	tabFiles
)

var tabNames = []string{"Identity", "Inbox", "Compose", "Tasks", "Peers", "Files"}

// ─── Messages (tea.Msg) ───────────────────────────────────────────────────────

type identityMsg struct{ id *pb.AgentIdentity }
type inboxMsg struct{ msgs []*pb.Message }
type newMessageMsg struct{ msg *pb.Message }
type tasksMsg struct{ tasks []*pb.Task }
type peersMsg struct{ peers []*pb.PeerInfo }
type healthMsg struct{ h *pb.HealthResponse }
type sendResultMsg struct {
	id  string
	err error
}
type errMsg struct{ err error }
type tickMsg time.Time

// ─── List item helpers ────────────────────────────────────────────────────────

type msgItem struct{ m *pb.Message }

func (i msgItem) Title() string {
	from := shortDID(i.m.FromDid)
	if from == "" {
		from = "unknown"
	}
	return fmt.Sprintf("%-20s  %s", from, kindLabel(i.m.Kind))
}
func (i msgItem) Description() string {
	t := time.UnixMilli(i.m.SentAt).Format("15:04:05")
	preview := ""
	if len(i.m.Payload) > 0 && len(i.m.Payload) < 80 {
		preview = string(i.m.Payload)
	}
	return fmt.Sprintf("%s  %s", t, preview)
}
func (i msgItem) FilterValue() string { return i.m.FromDid + i.m.Id }

type taskItem struct{ t *pb.Task }

func (i taskItem) Title() string {
	return fmt.Sprintf("%-36s  %s", i.t.Id[:min(8, len(i.t.Id))], i.t.Skill)
}
func (i taskItem) Description() string {
	return fmt.Sprintf("%-10s  assignee: %s", statusLabel(i.t.Status), shortDID(i.t.Assignee))
}
func (i taskItem) FilterValue() string { return i.t.Id + i.t.Skill }

type peerItem struct{ p *pb.PeerInfo }

func (i peerItem) Title() string       { return shortPeer(i.p.PeerId) }
func (i peerItem) Description() string { return fmt.Sprintf("latency: %dms  addrs: %d", i.p.LatencyMs, len(i.p.Addrs)) }
func (i peerItem) FilterValue() string { return i.p.PeerId }

// ─── Root model ───────────────────────────────────────────────────────────────

type model struct {
	client pb.A2ANodeClient
	conn   *grpc.ClientConn
	ctx    context.Context
	cancel context.CancelFunc

	width, height int
	activeTab     tab
	err           string
	statusMsg     string
	statusExpiry  time.Time

	// identity screen
	identity *pb.AgentIdentity
	health   *pb.HealthResponse

	// inbox screen
	inboxList   list.Model
	inboxMsgs   []*pb.Message
	selectedMsg *pb.Message

	// compose screen
	composeTo      textinput.Model
	composeText    textinput.Model
	composeFocus   int // 0=to, 1=text, 2=send
	composeSending bool
	composeSpinner spinner.Model

	// tasks screen
	taskList list.Model

	// peers screen
	peerList list.Model

	// files screen
	filePath    textinput.Model
	fileCID     textinput.Model
	fileFocus   int // 0=path, 1=send, 2=cid, 3=fetch
	fileResult  string
	fileSpinner spinner.Model
	fileBusy    bool

	// detail viewport
	detailView viewport.Model
	showDetail bool

	spinner spinner.Model
	loading bool
}

func newModel(client pb.A2ANodeClient, conn *grpc.ClientConn) model {
	ctx, cancel := context.WithCancel(context.Background())

	// inbox list
	inboxDelegate := list.NewDefaultDelegate()
	inboxDelegate.Styles.SelectedTitle = inboxDelegate.Styles.SelectedTitle.Foreground(colorAccent).BorderForeground(colorPrimary)
	inboxDelegate.Styles.SelectedDesc = inboxDelegate.Styles.SelectedDesc.Foreground(colorMuted).BorderForeground(colorPrimary)
	il := list.New(nil, inboxDelegate, 0, 0)
	il.Title = "Inbox"
	il.Styles.Title = styleTitle
	il.SetShowStatusBar(false)
	il.SetFilteringEnabled(true)

	// task list
	taskDelegate := list.NewDefaultDelegate()
	taskDelegate.Styles.SelectedTitle = taskDelegate.Styles.SelectedTitle.Foreground(colorAccent).BorderForeground(colorPrimary)
	tl := list.New(nil, taskDelegate, 0, 0)
	tl.Title = "Tasks"
	tl.Styles.Title = styleTitle
	tl.SetShowStatusBar(false)

	// peer list
	peerDelegate := list.NewDefaultDelegate()
	peerDelegate.Styles.SelectedTitle = peerDelegate.Styles.SelectedTitle.Foreground(colorAccent).BorderForeground(colorPrimary)
	pl := list.New(nil, peerDelegate, 0, 0)
	pl.Title = "Peers"
	pl.Styles.Title = styleTitle
	pl.SetShowStatusBar(false)

	// compose inputs
	toInput := textinput.New()
	toInput.Placeholder = "did:key:z6Mk..."
	toInput.CharLimit = 200
	toInput.Focus()

	textInput := textinput.New()
	textInput.Placeholder = "Type your message..."
	textInput.CharLimit = 2000

	// file inputs
	pathInput := textinput.New()
	pathInput.Placeholder = "/path/to/file"
	pathInput.CharLimit = 500

	cidInput := textinput.New()
	cidInput.Placeholder = "bafkrei..."
	cidInput.CharLimit = 200

	sp := spinner.New()
	sp.Spinner = spinner.Dot
	sp.Style = lipgloss.NewStyle().Foreground(colorPrimary)

	composeSpinner := spinner.New()
	composeSpinner.Spinner = spinner.Dot
	composeSpinner.Style = lipgloss.NewStyle().Foreground(colorPrimary)

	fileSpinner := spinner.New()
	fileSpinner.Spinner = spinner.Dot
	fileSpinner.Style = lipgloss.NewStyle().Foreground(colorPrimary)

	dv := viewport.New(0, 0)

	return model{
		client:         client,
		conn:           conn,
		ctx:            ctx,
		cancel:         cancel,
		inboxList:      il,
		taskList:       tl,
		peerList:       pl,
		composeTo:      toInput,
		composeText:    textInput,
		filePath:       pathInput,
		fileCID:        cidInput,
		spinner:        sp,
		composeSpinner: composeSpinner,
		fileSpinner:    fileSpinner,
		detailView:     dv,
		loading:        true,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		m.fetchIdentity(),
		m.fetchHealth(),
		m.fetchInbox(),
		m.fetchTasks(),
		m.fetchPeers(),
		m.subscribeInbox(),
		tea.Every(15*time.Second, func(t time.Time) tea.Msg { return tickMsg(t) }),
	)
}

// ─── Commands ─────────────────────────────────────────────────────────────────

func (m model) fetchIdentity() tea.Cmd {
	return func() tea.Msg {
		id, err := m.client.GetIdentity(m.ctx, &pb.Empty{})
		if err != nil {
			return errMsg{err}
		}
		return identityMsg{id}
	}
}

func (m model) fetchHealth() tea.Cmd {
	return func() tea.Msg {
		h, err := m.client.Health(m.ctx, &pb.Empty{})
		if err != nil {
			return errMsg{err}
		}
		return healthMsg{h}
	}
}

func (m model) fetchInbox() tea.Cmd {
	return func() tea.Msg {
		stream, err := m.client.GetInbox(m.ctx, &pb.InboxQuery{Limit: 100})
		if err != nil {
			return errMsg{err}
		}
		var msgs []*pb.Message
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			msgs = append(msgs, msg)
		}
		return inboxMsg{msgs}
	}
}

func (m model) subscribeInbox() tea.Cmd {
	return func() tea.Msg {
		stream, err := m.client.SubscribeInbox(m.ctx, &pb.SubscribeRequest{})
		if err != nil {
			return nil
		}
		msg, err := stream.Recv()
		if err != nil {
			return nil
		}
		return newMessageMsg{msg}
	}
}

func (m model) fetchTasks() tea.Cmd {
	return func() tea.Msg {
		// Fetch tasks via inbox side-channel — get all tasks from store
		// We use GetInbox stream for task-related messages as a proxy
		// Tasks are fetched via subscription; we use a placeholder approach
		return tasksMsg{nil}
	}
}

func (m model) fetchPeers() tea.Cmd {
	return func() tea.Msg {
		resp, err := m.client.ListPeers(m.ctx, &pb.Empty{})
		if err != nil {
			return errMsg{err}
		}
		return peersMsg{resp.Peers}
	}
}

func (m model) sendMessage(to, text string) tea.Cmd {
	return func() tea.Msg {
		msg := &pb.Message{
			ToDid:   to,
			Kind:    pb.MessageKind_MESSAGE_KIND_TEXT,
			Payload: []byte(text),
		}
		res, err := m.client.SendMessage(m.ctx, msg)
		if err != nil {
			return sendResultMsg{err: err}
		}
		return sendResultMsg{id: res.MessageId}
	}
}

func (m model) sendFile(path string) tea.Cmd {
	return func() tea.Msg {
		data, err := os.ReadFile(path)
		if err != nil {
			return sendResultMsg{err: fmt.Errorf("read file: %w", err)}
		}
		name := filepath.Base(path)
		res, err := m.client.SendFile(m.ctx, &pb.SendFileRequest{
			Data:     data,
			Name:     name,
			MimeType: "application/octet-stream",
		})
		if err != nil {
			return sendResultMsg{err: err}
		}
		return sendResultMsg{id: res.Cid}
	}
}

// ─── Update ───────────────────────────────────────────────────────────────────

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.relayout()

	case tea.KeyMsg:
		// Global keys
		switch msg.String() {
		case "ctrl+c", "q":
			if m.activeTab == tabCompose && (m.composeTo.Focused() || m.composeText.Focused()) {
				// don't quit while typing
			} else if m.activeTab == tabFiles && (m.filePath.Focused() || m.fileCID.Focused()) {
				// don't quit while typing
			} else {
				m.cancel()
				return m, tea.Quit
			}
		case "1":
			m.activeTab = tabIdentity
			cmds = append(cmds, m.fetchHealth())
		case "2":
			m.activeTab = tabInbox
			cmds = append(cmds, m.fetchInbox())
		case "3":
			m.activeTab = tabCompose
			m.composeTo.Focus()
			m.composeText.Blur()
			m.composeFocus = 0
		case "4":
			m.activeTab = tabTasks
		case "5":
			m.activeTab = tabPeers
			cmds = append(cmds, m.fetchPeers())
		case "6":
			m.activeTab = tabFiles
		case "esc":
			if m.showDetail {
				m.showDetail = false
			}
		}

		// Tab-specific keys
		switch m.activeTab {
		case tabInbox:
			if msg.String() == "enter" && !m.showDetail {
				if item, ok := m.inboxList.SelectedItem().(msgItem); ok {
					m.selectedMsg = item.m
					m.showDetail = true
					m.detailView.SetContent(formatMessage(item.m))
				}
			} else if !m.showDetail {
				var cmd tea.Cmd
				m.inboxList, cmd = m.inboxList.Update(msg)
				cmds = append(cmds, cmd)
			}

		case tabCompose:
			cmds = append(cmds, m.updateCompose(msg)...)

		case tabTasks:
			var cmd tea.Cmd
			m.taskList, cmd = m.taskList.Update(msg)
			cmds = append(cmds, cmd)

		case tabPeers:
			var cmd tea.Cmd
			m.peerList, cmd = m.peerList.Update(msg)
			cmds = append(cmds, cmd)

		case tabFiles:
			cmds = append(cmds, m.updateFiles(msg)...)
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd)
		if m.composeSending {
			m.composeSpinner, cmd = m.composeSpinner.Update(msg)
			cmds = append(cmds, cmd)
		}
		if m.fileBusy {
			m.fileSpinner, cmd = m.fileSpinner.Update(msg)
			cmds = append(cmds, cmd)
		}

	case tickMsg:
		cmds = append(cmds,
			m.fetchHealth(),
			m.fetchPeers(),
			tea.Every(15*time.Second, func(t time.Time) tea.Msg { return tickMsg(t) }),
		)

	case identityMsg:
		m.identity = msg.id
		m.loading = false

	case healthMsg:
		m.health = msg.h

	case inboxMsg:
		m.inboxMsgs = msg.msgs
		items := make([]list.Item, len(msg.msgs))
		for i, m := range msg.msgs {
			items[i] = msgItem{m}
		}
		m.inboxList.SetItems(items)

	case newMessageMsg:
		m.inboxMsgs = append([]*pb.Message{msg.msg}, m.inboxMsgs...)
		items := make([]list.Item, len(m.inboxMsgs))
		for i, msg := range m.inboxMsgs {
			items[i] = msgItem{msg}
		}
		m.inboxList.SetItems(items)
		m.setStatus(styleSuccess.Render("● New message from " + shortDID(msg.msg.FromDid)))
		// re-subscribe
		cmds = append(cmds, m.subscribeInbox())

	case tasksMsg:
		items := make([]list.Item, len(msg.tasks))
		for i, t := range msg.tasks {
			items[i] = taskItem{t}
		}
		m.taskList.SetItems(items)

	case peersMsg:
		items := make([]list.Item, len(msg.peers))
		for i, p := range msg.peers {
			items[i] = peerItem{p}
		}
		m.peerList.SetItems(items)

	case sendResultMsg:
		m.composeSending = false
		m.fileBusy = false
		if msg.err != nil {
			m.setStatus(styleDanger.Render("✗ " + msg.err.Error()))
		} else {
			m.setStatus(styleSuccess.Render("✓ Sent: " + msg.id))
			if m.activeTab == tabCompose {
				m.composeTo.SetValue("")
				m.composeText.SetValue("")
				m.composeFocus = 0
				m.composeTo.Focus()
				m.composeText.Blur()
			}
			if m.activeTab == tabFiles {
				m.fileResult = msg.id
			}
		}

	case errMsg:
		if msg.err != nil && msg.err != context.Canceled {
			m.setStatus(styleDanger.Render("✗ " + msg.err.Error()))
		}
		m.loading = false
	}

	// Viewport update
	if m.showDetail {
		var cmd tea.Cmd
		m.detailView, cmd = m.detailView.Update(msg)
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m *model) updateCompose(msg tea.KeyMsg) []tea.Cmd {
	var cmds []tea.Cmd
	switch msg.String() {
	case "tab":
		m.composeFocus = (m.composeFocus + 1) % 3
		switch m.composeFocus {
		case 0:
			m.composeTo.Focus()
			m.composeText.Blur()
		case 1:
			m.composeTo.Blur()
			m.composeText.Focus()
		case 2:
			m.composeTo.Blur()
			m.composeText.Blur()
		}
	case "enter":
		if m.composeFocus == 2 || (m.composeFocus == 1 && msg.String() == "enter") {
			to := strings.TrimSpace(m.composeTo.Value())
			text := strings.TrimSpace(m.composeText.Value())
			if to != "" && text != "" && !m.composeSending {
				m.composeSending = true
				cmds = append(cmds, m.composeSpinner.Tick, m.sendMessage(to, text))
			}
		}
		if m.composeFocus == 0 {
			m.composeFocus = 1
			m.composeTo.Blur()
			m.composeText.Focus()
		}
	default:
		var cmd tea.Cmd
		switch m.composeFocus {
		case 0:
			m.composeTo, cmd = m.composeTo.Update(msg)
		case 1:
			m.composeText, cmd = m.composeText.Update(msg)
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

func (m *model) updateFiles(msg tea.KeyMsg) []tea.Cmd {
	var cmds []tea.Cmd
	switch msg.String() {
	case "tab":
		m.fileFocus = (m.fileFocus + 1) % 4
		switch m.fileFocus {
		case 0:
			m.filePath.Focus()
			m.fileCID.Blur()
		case 1:
			m.filePath.Blur()
			m.fileCID.Blur()
		case 2:
			m.filePath.Blur()
			m.fileCID.Focus()
		case 3:
			m.filePath.Blur()
			m.fileCID.Blur()
		}
	case "enter":
		switch m.fileFocus {
		case 0:
			m.fileFocus = 1
			m.filePath.Blur()
		case 1:
			path := strings.TrimSpace(m.filePath.Value())
			if path != "" && !m.fileBusy {
				m.fileBusy = true
				m.fileResult = ""
				cmds = append(cmds, m.fileSpinner.Tick, m.sendFile(path))
			}
		case 2:
			m.fileFocus = 3
			m.fileCID.Blur()
		}
	default:
		var cmd tea.Cmd
		switch m.fileFocus {
		case 0:
			m.filePath, cmd = m.filePath.Update(msg)
		case 2:
			m.fileCID, cmd = m.fileCID.Update(msg)
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}

func (m *model) relayout() {
	contentH := m.height - 4 // tab bar + status bar
	listW := m.width - 4

	m.inboxList.SetSize(listW, contentH-2)
	m.taskList.SetSize(listW, contentH-2)
	m.peerList.SetSize(listW, contentH-2)
	m.detailView.Width = m.width - 6
	m.detailView.Height = contentH - 4
	m.composeTo.Width = m.width - 20
	m.composeText.Width = m.width - 20
	m.filePath.Width = m.width - 20
	m.fileCID.Width = m.width - 20
}

func (m *model) setStatus(s string) {
	m.statusMsg = s
	m.statusExpiry = time.Now().Add(4 * time.Second)
}

// ─── View ─────────────────────────────────────────────────────────────────────

func (m model) View() string {
	if m.width == 0 {
		return "Loading…"
	}

	tabs := m.renderTabs()
	status := m.renderStatus()

	var content string
	switch m.activeTab {
	case tabIdentity:
		content = m.viewIdentity()
	case tabInbox:
		content = m.viewInbox()
	case tabCompose:
		content = m.viewCompose()
	case tabTasks:
		content = m.viewTasks()
	case tabPeers:
		content = m.viewPeers()
	case tabFiles:
		content = m.viewFiles()
	}

	return lipgloss.JoinVertical(lipgloss.Left, tabs, content, status)
}

func (m model) renderTabs() string {
	var tabs []string
	for i, name := range tabNames {
		label := fmt.Sprintf("%d:%s", i+1, name)
		if tab(i) == m.activeTab {
			tabs = append(tabs, styleTabActive.Render(label))
		} else {
			tabs = append(tabs, styleTab.Render(label))
		}
	}
	bar := lipgloss.JoinHorizontal(lipgloss.Top, tabs...)
	return styleTabBar.Width(m.width).Render(bar)
}

func (m model) renderStatus() string {
	msg := ""
	if time.Now().Before(m.statusExpiry) {
		msg = m.statusMsg
	}

	peers := ""
	if m.health != nil {
		peers = styleMuted.Render(fmt.Sprintf("peers: %d  uptime: %s", m.health.PeerCount, fmtDuration(time.Duration(m.health.UptimeSecs)*time.Second)))
	}

	left := msg
	right := peers
	gap := m.width - lipgloss.Width(left) - lipgloss.Width(right) - 2
	if gap < 0 {
		gap = 0
	}
	return styleStatusBar.Width(m.width).Render(
		left + strings.Repeat(" ", gap) + right,
	)
}

func (m model) viewIdentity() string {
	if m.loading {
		return lipgloss.NewStyle().Padding(2).Render(m.spinner.View() + "  Connecting to daemon…")
	}
	if m.identity == nil {
		return lipgloss.NewStyle().Padding(2).Render(styleDanger.Render("✗ Not connected to daemon"))
	}

	contentH := m.height - 4
	panel := stylePanelActive.Width(m.width - 4).Height(contentH - 2)

	var rows []string
	rows = append(rows, styleTitle.Render("Node Identity"))
	rows = append(rows, row("DID", styleDID.Render(m.identity.Did)))
	rows = append(rows, row("Public Key", styleValue.Render(truncate(m.identity.PublicKey, 48))))
	rows = append(rows, "")
	rows = append(rows, styleTitle.Render("Addresses"))
	for _, addr := range m.identity.Multiaddrs {
		rows = append(rows, "  "+styleMuted.Render("→")+" "+styleValue.Render(addr))
	}

	if m.health != nil {
		rows = append(rows, "")
		rows = append(rows, styleTitle.Render("Health"))
		rows = append(rows, row("Version", styleValue.Render(m.health.Version)))
		rows = append(rows, row("Peers", styleValue.Render(fmt.Sprintf("%d", m.health.PeerCount))))
		rows = append(rows, row("Uptime", styleValue.Render(fmtDuration(time.Duration(m.health.UptimeSecs)*time.Second))))
		status := styleSuccess.Render("● online")
		if !m.health.Ok {
			status = styleDanger.Render("● degraded")
		}
		rows = append(rows, row("Status", status))
	}

	return lipgloss.NewStyle().Padding(1).Render(
		panel.Render(strings.Join(rows, "\n")),
	)
}

func (m model) viewInbox() string {
	contentH := m.height - 4

	if m.showDetail && m.selectedMsg != nil {
		header := styleTitle.Render("Message Detail")
		from := row("From", styleDID.Render(m.selectedMsg.FromDid))
		kind := row("Kind", styleValue.Render(kindLabel(m.selectedMsg.Kind)))
		ts := row("Time", styleValue.Render(time.UnixMilli(m.selectedMsg.SentAt).Format("2006-01-02 15:04:05")))
		hint := styleMuted.Render("esc: back")

		m.detailView.Width = m.width - 6
		m.detailView.Height = contentH - 10

		detail := stylePanel.Width(m.width - 4).Render(
			lipgloss.JoinVertical(lipgloss.Left,
				header, from, kind, ts, "",
				m.detailView.View(), "",
				hint,
			),
		)
		return lipgloss.NewStyle().Padding(1).Render(detail)
	}

	m.inboxList.SetSize(m.width-4, contentH-4)
	hint := styleMuted.Render("enter: view  /: filter  esc: clear")
	return lipgloss.NewStyle().Padding(1).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			m.inboxList.View(),
			hint,
		),
	)
}

func (m model) viewCompose() string {
	contentH := m.height - 4
	_ = contentH

	focusedInput := func(label string, ti textinput.Model, focused bool) string {
		st := stylePanel
		if focused {
			st = stylePanelActive
		}
		return st.Width(m.width - 6).Render(
			lipgloss.JoinVertical(lipgloss.Left,
				styleMuted.Render(label),
				ti.View(),
			),
		)
	}

	sendBtn := styleButtonMuted.Render("[ Send ]")
	if m.composeFocus == 2 {
		sendBtn = styleButton.Render("[ Send ]")
	}
	if m.composeSending {
		sendBtn = m.composeSpinner.View() + " Sending…"
	}

	hint := styleMuted.Render("tab: next field  enter: next / send")

	return lipgloss.NewStyle().Padding(1).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			styleTitle.Render("Send Message"),
			"",
			focusedInput("To (DID)", m.composeTo, m.composeFocus == 0),
			"",
			focusedInput("Message", m.composeText, m.composeFocus == 1),
			"",
			sendBtn,
			"",
			hint,
		),
	)
}

func (m model) viewTasks() string {
	contentH := m.height - 4
	m.taskList.SetSize(m.width-4, contentH-4)
	hint := styleMuted.Render("No task subscription yet — tasks appear when created via CLI")
	if len(m.taskList.Items()) > 0 {
		hint = styleMuted.Render("↑/↓: navigate  enter: detail")
	}
	return lipgloss.NewStyle().Padding(1).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			m.taskList.View(),
			hint,
		),
	)
}

func (m model) viewPeers() string {
	contentH := m.height - 4
	m.peerList.SetSize(m.width-4, contentH-4)
	hint := styleMuted.Render("auto-refreshes every 15s  •  5: refresh now")
	return lipgloss.NewStyle().Padding(1).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			m.peerList.View(),
			hint,
		),
	)
}

func (m model) viewFiles() string {
	focusedInput := func(label string, ti textinput.Model, focused bool) string {
		st := stylePanel
		if focused {
			st = stylePanelActive
		}
		return st.Width(m.width - 6).Render(
			lipgloss.JoinVertical(lipgloss.Left,
				styleMuted.Render(label),
				ti.View(),
			),
		)
	}

	sendBtn := styleButtonMuted.Render("[ Upload ]")
	if m.fileFocus == 1 {
		sendBtn = styleButton.Render("[ Upload ]")
	}

	result := ""
	if m.fileResult != "" {
		result = styleSuccess.Render("✓ CID: " + m.fileResult)
	}
	if m.fileBusy {
		result = m.fileSpinner.View() + " Working…"
	}

	hint := styleMuted.Render("tab: next field  enter: confirm / send")

	return lipgloss.NewStyle().Padding(1).Render(
		lipgloss.JoinVertical(lipgloss.Left,
			styleTitle.Render("Files"),
			"",
			focusedInput("File Path (upload)", m.filePath, m.fileFocus == 0),
			"",
			sendBtn,
			result,
			"",
			lipgloss.NewStyle().Foreground(colorBorder).Render(strings.Repeat("─", m.width-8)),
			"",
			focusedInput("CID (fetch)", m.fileCID, m.fileFocus == 2),
			"",
			hint,
		),
	)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func row(label, value string) string {
	return styleLabel.Render(label+":") + " " + value
}

func shortDID(did string) string {
	if len(did) <= 20 {
		return did
	}
	return did[:16] + "…"
}

func shortPeer(id string) string {
	if len(id) <= 20 {
		return id
	}
	return id[:8] + "…" + id[len(id)-8:]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func kindLabel(k pb.MessageKind) string {
	switch k {
	case pb.MessageKind_MESSAGE_KIND_TEXT:
		return styleSuccess.Render("text")
	case pb.MessageKind_MESSAGE_KIND_TASK_REQUEST:
		return styleWarning.Render("task_request")
	case pb.MessageKind_MESSAGE_KIND_TASK_EVENT:
		return styleWarning.Render("task_event")
	case pb.MessageKind_MESSAGE_KIND_TASK_RESULT:
		return styleSuccess.Render("task_result")
	case pb.MessageKind_MESSAGE_KIND_ACK:
		return styleMuted.Render("ack")
	default:
		return styleMuted.Render("unknown")
	}
}

func statusLabel(s pb.TaskStatus) string {
	switch s {
	case pb.TaskStatus_TASK_STATUS_SUBMITTED:
		return styleMuted.Render("submitted")
	case pb.TaskStatus_TASK_STATUS_WORKING:
		return styleWarning.Render("working")
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		return styleSuccess.Render("completed")
	case pb.TaskStatus_TASK_STATUS_FAILED:
		return styleDanger.Render("failed")
	case pb.TaskStatus_TASK_STATUS_CANCELLED:
		return styleMuted.Render("cancelled")
	default:
		return styleMuted.Render("unknown")
	}
}

func formatMessage(m *pb.Message) string {
	var sb strings.Builder
	sb.WriteString("ID:      " + m.Id + "\n")
	sb.WriteString("From:    " + m.FromDid + "\n")
	sb.WriteString("To:      " + m.ToDid + "\n")
	sb.WriteString("Thread:  " + m.ThreadId + "\n")
	sb.WriteString("Task:    " + m.TaskId + "\n")
	sb.WriteString("Kind:    " + m.Kind.String() + "\n")
	sb.WriteString("Sent:    " + time.UnixMilli(m.SentAt).Format(time.RFC3339) + "\n")
	sb.WriteString("\nPayload:\n")
	if len(m.Payload) > 0 {
		sb.WriteString(string(m.Payload))
	} else {
		sb.WriteString("(empty)")
	}
	return sb.String()
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	min := d / time.Minute
	d -= min * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, min, s)
	}
	if min > 0 {
		return fmt.Sprintf("%dm%02ds", min, s)
	}
	return fmt.Sprintf("%ds", s)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	var (
		dataDir  = flag.String("data-dir", "", "Data directory (default: ~/.moltmesh)")
		grpcAddr = flag.String("grpc-addr", "", "gRPC address (default: unix socket in data-dir)")
	)
	flag.Parse()

	// resolve data dir
	if *dataDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Fprintln(os.Stderr, "error: could not determine home dir:", err)
			os.Exit(1)
		}
		*dataDir = filepath.Join(home, ".moltmesh")
	}
	if *grpcAddr == "" {
		*grpcAddr = filepath.Join(*dataDir, "a2a.sock")
	}

	// dial gRPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dialTarget string
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())

	if (*grpcAddr)[0] == '/' {
		dialTarget = "unix:" + *grpcAddr
	} else {
		dialTarget = *grpcAddr
	}

	conn, err := grpc.DialContext(ctx, dialTarget, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: cannot connect to daemon at %s\n  → run 'moltmesh-daemon start' first\n  → error: %v\n", *grpcAddr, err)
		os.Exit(1)
	}

	client := pb.NewA2ANodeClient(conn)

	p := tea.NewProgram(
		newModel(client, conn),
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		conn.Close()
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	conn.Close()
}
