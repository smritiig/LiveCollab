import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";

function RoomPage() {
  const { roomId } = useParams();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  const username = searchParams.get("username");

  const socketRef = useRef<WebSocket | null>(null);
  const debounceTimeoutRef = useRef<number | null>(null);
  const typingTimeoutRef = useRef<number | null>(null);

  const [pendingUsername, setPendingUsername] = useState("");
  const [status, setStatus] = useState("🟡 Connecting...");
  const [users, setUsers] = useState<string[]>([]);
  const [content, setContent] = useState("");
  const [copied, setCopied] = useState(false);
  const [lastUpdatedBy, setLastUpdatedBy] = useState<string | null>(null);
  const [typingUser, setTypingUser] = useState<string | null>(null);

  const shareLink = useMemo(() => {
    if (!roomId) return "";
    return `${window.location.origin}/room/${roomId}`;
  }, [roomId]);

  useEffect(() => {
    if (!roomId || !username) return;

    const ws = new WebSocket(
      `ws://localhost:8080/ws?roomId=${roomId}&username=${encodeURIComponent(
        username
      )}`
    );

    socketRef.current = ws;

    ws.onopen = () => {
      setStatus("🟢 Connected");
    };

    ws.onclose = () => {
      setStatus("🔴 Disconnected");
    };

    ws.onerror = () => {
      setStatus("🔴 Error");
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        switch (data.type) {
          case "room_state":
            setContent(data.content || "");
            setUsers(data.users || []);
            break;

          case "presence_update":
            setUsers(data.users || []);
            break;

          case "content_update":
            setContent(data.content || "");
            if (data.username) {
              setLastUpdatedBy(data.username);
            }
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

          case "system":
            console.log("System:", data.content);
            break;

          default:
            console.log("Unknown message:", data);
        }
      } catch (err) {
        console.error("Invalid message:", err);
      }
    };

    return () => {
      if (debounceTimeoutRef.current) {
        window.clearTimeout(debounceTimeoutRef.current);
      }

      if (typingTimeoutRef.current) {
        window.clearTimeout(typingTimeoutRef.current);
      }

      ws.close();
    };
  }, [roomId, username]);

  const handleCopyLink = async () => {
    if (!shareLink) return;

    try {
      await navigator.clipboard.writeText(shareLink);
      setCopied(true);

      window.setTimeout(() => {
        setCopied(false);
      }, 1500);
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
          <div className="status">{status}</div>

          <div className="share-box">
            <input type="text" value={shareLink} readOnly />
            <button onClick={handleCopyLink}>
              {copied ? "Copied!" : "Copy Link"}
            </button>
          </div>
        </div>
      </div>

      <div className="room-container">
        <div className="editor">
          <div className="editor-meta">
            {lastUpdatedBy && (
              <div className="meta-pill secondary">
                Last updated by <strong>{lastUpdatedBy}</strong>
              </div>
            )}

            {typingUser && (
              <div className="meta-pill">
                {typingUser} is typing...
              </div>
            )}
          </div>

          <textarea
            value={content}
            onChange={(e) => {
              const newContent = e.target.value;
              setContent(newContent);
              setLastUpdatedBy(username || null);

              if (
                socketRef.current &&
                socketRef.current.readyState === WebSocket.OPEN
              ) {
                socketRef.current.send(
                  JSON.stringify({
                    type: "typing",
                  })
                );
              }

              if (debounceTimeoutRef.current) {
                window.clearTimeout(debounceTimeoutRef.current);
              }

              debounceTimeoutRef.current = window.setTimeout(() => {
                if (
                  socketRef.current &&
                  socketRef.current.readyState === WebSocket.OPEN
                ) {
                  socketRef.current.send(
                    JSON.stringify({
                      type: "content_update",
                      content: newContent,
                    })
                  );
                }
              }, 250);
            }}
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