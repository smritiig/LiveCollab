import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { API_BASE_URL } from "../config";

function HomePage() {
  const navigate = useNavigate();

  const [username, setUsername] = useState("");
  const [roomId, setRoomId] = useState("");
  const [isCreating, setIsCreating] = useState(false);

  const handleJoinRoom = () => {
    if (!username.trim() || !roomId.trim()) {
      alert("Please enter both username and room ID.");
      return;
    }

    navigate(`/room/${roomId}?username=${encodeURIComponent(username)}`);
  };

  const handleCreateRoom = async () => {
    if (!username.trim()) {
      alert("Please enter a username.");
      return;
    }

    try {
      setIsCreating(true);

      const response = await fetch(`${API_BASE_URL}/rooms`, {
        method: "POST",
      });

      if (!response.ok) {
        throw new Error("Failed to create room");
      }

      const data: { roomId: string } = await response.json();

      navigate(`/room/${data.roomId}?username=${encodeURIComponent(username)}`);
    } catch (error) {
      console.error(error);
      alert("Failed to create room.");
    } finally {
      setIsCreating(false);
    }
  };

  return (
    <div className="landing-page">
      <main className="landing-shell">
        <section className="landing-hero">
          <div className="landing-badge">
            Distributed real-time collaboration
          </div>

          <h1 className="landing-title">LiveCollab</h1>

          <p className="landing-subtitle">
            A fault-tolerant collaborative editor built with Go, WebSockets,
            Redis Streams, and ordered event replay.
          </p>

          <div className="landing-features">
            <span>Multi-node realtime</span>
            <span>Ordered reconnect replay</span>
            <span>Failure recovery</span>
          </div>
        </section>

        <section className="landing-card">
          <div className="landing-card-heading">
            <h2>Start a session</h2>
            <p>Create a new room or join an existing one.</p>
          </div>

          <label className="landing-label">Your name</label>

          <input
            className="landing-input"
            type="text"
            placeholder="e.g. Smriti"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />

          <button
            className="landing-primary-button"
            onClick={handleCreateRoom}
            disabled={isCreating}
          >
            {isCreating ? "Creating room..." : "Create new room"}
          </button>

          <div className="landing-divider">
            <span>or join an existing room</span>
          </div>

          <div className="landing-join-row">
            <input
              className="landing-input"
              type="text"
              placeholder="Room ID"
              value={roomId}
              onChange={(e) => setRoomId(e.target.value)}
            />

            <button
              className="landing-secondary-button"
              onClick={handleJoinRoom}
            >
              Join room
            </button>
          </div>
        </section>

        <div className="landing-stack">
          <span>Go</span>
          <span>WebSockets</span>
          <span>Redis Streams</span>
          <span>OpenTelemetry</span>
        </div>
      </main>
    </div>
  );
}

export default HomePage;