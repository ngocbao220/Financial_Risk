import React, { useEffect, useState } from "react";

function App() {
  const [risks, setRisks] = useState([]);

  useEffect(() => {
    const ws = new WebSocket("ws://localhost:8000/ws/risks");
    ws.onmessage = (e) => setRisks(JSON.parse(e.data));
    return () => ws.close();
  }, []);

  return (
    <div style={{ padding: 20 }}>
      <h1>Operational Risk Dashboard</h1>
      <table border="1" cellPadding="6">
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Date</th>
            <th>Risk</th>
            <th>Risk (Adj.)</th>
            <th>Level</th>
          </tr>
        </thead>
        <tbody>
          {risks.map((r, i) => (
            <tr key={i} style={{ color: r.risk_level === "HIGH" ? "red" : r.risk_level === "MEDIUM" ? "orange" : "black" }}>
              <td>{r.ticker}</td>
              <td>{r.date}</td>
              <td>{r.risk?.toFixed(3)}</td>
              <td>{r.risk_ind_adjs?.toFixed(3)}</td>
              <td>{r.risk_level}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
