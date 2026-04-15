import { useState } from "react";
import { useNavigate } from "react-router-dom";

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

      const response = await fetch("http://localhost:8080/rooms", {
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
      <div className="landing-shell">
        <div className="landing-badge">Real-time collaboration</div>

        <h1 className="landing-title">LiveCollab</h1>

        <p className="landing-subtitle">
          Create a room, invite others, and edit together in real time with
          shared state, presence tracking, and live updates.
        </p>

        <div className="landing-card">
          <label className="landing-label">Your name</label>
          <input
            className="landing-input"
            type="text"
            placeholder="Enter your name"
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

          <div className="landing-or">or join an existing room</div>

          <div className="landing-join-row">
            <input
              className="landing-input"
              type="text"
              placeholder="Enter room ID"
              value={roomId}
              onChange={(e) => setRoomId(e.target.value)}
            />
            <button className="landing-secondary-button" onClick={handleJoinRoom}>
              Join
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default HomePage;