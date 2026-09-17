import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { WEBSOCKET_BASE_URL } from "../config";

function createClientId(username: string | null) {
  const suffix =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID()
      : `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  return `${username || "anonymous"}-${suffix}`;
}

function RoomPage() {
  const { roomId } = useParams();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const username = searchParams.get("username");

  const socketRef = useRef<WebSocket | null>(null);
  const debounceTimeoutRef = useRef<number | null>(null);
  const typingTimeoutRef = useRef<number | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const serverVersionRef = useRef(0);
  const lastSequenceRef = useRef(0);
  const operationCounterRef = useRef(0);
  const clientIdRef = useRef(createClientId(username));

  const [pendingUsername, setPendingUsername] = useState("");
  const [status, setStatus] = useState("🟡 Connecting...");
  const [users, setUsers] = useState<string[]>([]);
  const [content, setContent] = useState("");
  const [serverVersion, setServerVersion] = useState(0);
  const [lastSequence, setLastSequence] = useState(0);
  const [copied, setCopied] = useState(false);
  const [lastUpdatedBy, setLastUpdatedBy] = useState<string | null>(null);
  const [lastOperationStatus, setLastOperationStatus] = useState<string | null>(null);
  const [typingUser, setTypingUser] = useState<string | null>(null);

  const shareLink = useMemo(() => {
    if (!roomId) return "";
    return `${window.location.origin}/room/${roomId}`;
  }, [roomId]);

  const updateServerVersion = (nextVersion: unknown) => {
    if (typeof nextVersion !== "number") return;
    serverVersionRef.current = nextVersion;
    setServerVersion(nextVersion);
  };

  const updateSequence = (nextSequence: unknown) => {
    if (typeof nextSequence !== "number") return;
    lastSequenceRef.current = nextSequence;
    setLastSequence(nextSequence);
  };

  useEffect(() => {
    if (!roomId || !username) return;

    const storageKey = `livecollab:${roomId}:${username}:clientId`;
    const storedClientId = window.sessionStorage.getItem(storageKey);
    clientIdRef.current = storedClientId || createClientId(username);
    window.sessionStorage.setItem(storageKey, clientIdRef.current);
    operationCounterRef.current = 0;

    let disposed = false;
    let reconnectAttempt = 0;

    const connect = () => {
      if (disposed) return;

      const query = new URLSearchParams({
        roomId,
        username,
        clientId: clientIdRef.current,
        lastSequence: String(lastSequenceRef.current),
      });
      const ws = new WebSocket(`${WEBSOCKET_BASE_URL}/ws?${query.toString()}`);
      socketRef.current = ws;
      setStatus(reconnectAttempt === 0 ? "🟡 Connecting..." : "🟡 Reconnecting...");

      ws.onopen = () => {
        reconnectAttempt = 0;
        setStatus("🟢 Connected");
      };

      ws.onclose = () => {
        if (disposed || socketRef.current !== ws) return;
        socketRef.current = null;
        setStatus("🟡 Reconnecting...");
        const delayMs = Math.min(300 * 2 ** reconnectAttempt, 3000);
        reconnectAttempt += 1;
        reconnectTimeoutRef.current = window.setTimeout(connect, delayMs);
      };

      ws.onerror = () => {
        if (socketRef.current === ws) {
          setStatus("🟠 Connection interrupted");
        }
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          switch (data.type) {
            case "room_state":
              setContent(data.content || "");
              setUsers(data.users || []);
              updateServerVersion(data.serverVersion ?? 0);
              updateSequence(data.sequence ?? 0);
              break;

            case "presence_update":
              setUsers(data.users || []);
              break;

            case "content_update": {
              const incomingSequence =
                typeof data.sequence === "number" ? data.sequence : null;
              if (
                incomingSequence !== null &&
                incomingSequence <= lastSequenceRef.current
              ) {
                break;
              }
              if (
                incomingSequence !== null &&
                lastSequenceRef.current > 0 &&
                incomingSequence > lastSequenceRef.current + 1
              ) {
                setStatus(
                  `🟠 Event gap: expected ${lastSequenceRef.current + 1}, received ${incomingSequence}`
                );
              }
              setContent(data.content || "");
              updateServerVersion(data.serverVersion);
              if (incomingSequence !== null) updateSequence(incomingSequence);
              if (data.username) setLastUpdatedBy(data.username);
              break;
            }

            case "operation_result":
              updateServerVersion(data.serverVersion);
              if (data.accepted === false) {
                setLastOperationStatus(
                  `Conflict detected (${data.reason || "operation rejected"})`
                );
              } else if (data.accepted === true) {
                setLastOperationStatus(
                  `Saved as version ${data.serverVersion} · event ${data.sequence}`
                );
              }
              break;

            case "resume_started":
              setStatus(
                `🟡 Restoring events after ${data.fromSequence ?? lastSequenceRef.current}...`
              );
              break;

            case "resume_complete":
              updateServerVersion(data.serverVersion);
              if (typeof data.latestSequence === "number") {
                updateSequence(data.latestSequence);
              }
              setStatus(`🟢 Synchronized · replayed ${data.replayed ?? 0} events`);
              break;

            case "typing":
              if (data.username && data.username !== username) {
                setTypingUser(data.username);
                if (typingTimeoutRef.current) {
                  window.clearTimeout(typingTimeoutRef.current);
                }
                typingTimeoutRef.current = window.setTimeout(() => {
                  setTypingUser(null);
                }, 1200);
              }
              break;

            case "error":
              setLastOperationStatus(`Server error: ${data.reason || "unknown"}`);
              break;

            default:
              console.log("Unknown message:", data);
          }
        } catch (err) {
          console.error("Invalid message:", err);
        }
      };
    };

    connect();

    return () => {
      disposed = true;
      if (debounceTimeoutRef.current) window.clearTimeout(debounceTimeoutRef.current);
      if (typingTimeoutRef.current) window.clearTimeout(typingTimeoutRef.current);
      if (reconnectTimeoutRef.current) window.clearTimeout(reconnectTimeoutRef.current);
      const socket = socketRef.current;
      socketRef.current = null;
      socket?.close();
    };
  }, [roomId, username]);

  const handleCopyLink = async () => {
    if (!shareLink) return;
    try {
      await navigator.clipboard.writeText(shareLink);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (error) {
      console.error("Copy failed:", error);
      alert("Could not copy link.");
    }
  };

  const handleJoinWithUsername = () => {
    if (!pendingUsername.trim() || !roomId) {
      alert("Please enter a username.");
      return;
    }
    navigate(`/room/${roomId}?username=${encodeURIComponent(pendingUsername)}`);
  };

  const handleContentChange = (newContent: string) => {
    setContent(newContent);
    setLastUpdatedBy(username || null);

    if (socketRef.current?.readyState === WebSocket.OPEN) {
      socketRef.current.send(JSON.stringify({ type: "typing" }));
    }

    if (debounceTimeoutRef.current) {
      window.clearTimeout(debounceTimeoutRef.current);
    }

    const baseVersion = serverVersionRef.current;
    const operationId = `${clientIdRef.current}:${++operationCounterRef.current}`;

    debounceTimeoutRef.current = window.setTimeout(() => {
      if (socketRef.current?.readyState === WebSocket.OPEN) {
        socketRef.current.send(
          JSON.stringify({
            type: "content_update",
            operationId,
            clientId: clientIdRef.current,
            baseVersion,
            content: newContent,
          })
        );
      } else {
        setLastOperationStatus("Edit not sent while disconnected; retry after synchronization.");
      }
    }, 250);
  };

  if (!username) {
    return (
      <div className="home-page">
        <div className="home-card">
          <h1>Join Room</h1>
          <p>
            Enter your name to join room <strong>{roomId}</strong>.
          </p>
          <input
            type="text"
            placeholder="Enter your username"
            value={pendingUsername}
            onChange={(e) => setPendingUsername(e.target.value)}
          />
          <button onClick={handleJoinWithUsername} style={{ width: "100%" }}>
            Join Room
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="room-page">
      <div className="room-header">
        <div className="room-header-left">
          <h2>LiveCollab</h2>
          <p>
            Room: <strong>{roomId}</strong>
          </p>
          <p>
            You joined as <strong>{username}</strong>
          </p>
        </div>

        <div className="room-header-right">
          <div className="status" data-testid="connection-status">
            {status}
          </div>
          <div className="share-box">
            <input type="text" value={shareLink} readOnly />
            <button onClick={handleCopyLink}>{copied ? "Copied!" : "Copy Link"}</button>
          </div>
        </div>
      </div>

      <div className="room-container">
        <div className="editor">
          <div className="editor-meta">
            <div className="meta-pill secondary" data-testid="document-version">
              Version {serverVersion}
            </div>
            <div className="meta-pill secondary" data-testid="event-sequence">
              Event {lastSequence}
            </div>
            {lastUpdatedBy && (
              <div className="meta-pill secondary">
                Last updated by <strong>{lastUpdatedBy}</strong>
              </div>
            )}
            {lastOperationStatus && (
              <div className="meta-pill" data-testid="operation-status">
                {lastOperationStatus}
              </div>
            )}
            {typingUser && <div className="meta-pill">{typingUser} is typing...</div>}
          </div>

          <textarea
            data-testid="document-editor"
            value={content}
            onChange={(e) => handleContentChange(e.target.value)}
            placeholder="Start collaborating in real time..."
          />
        </div>

        <div className="sidebar">
          <div className="sidebar-header">
            <h3>Online Users</h3>
            <span className="user-count">{users.length} active</span>
          </div>
          {users.length === 0 ? (
            <div className="sidebar-empty">
              Nobody is connected yet. Share the room link to invite someone in.
            </div>
          ) : (
            <ul>
              {users.map((u, i) => (
                <li key={i}>
                  <span className="user-dot" />
                  <span>{u}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

export default RoomPage;
