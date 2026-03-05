import { useEffect, useState } from "react";
import axios from "axios";
import { Bar, Line } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
} from "chart.js";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
);

function App() {
  const [realtime, setRealtime] = useState([]);
  const [batchState, setBatchState] = useState([]);
  const [batchMonthly, setBatchMonthly] = useState([]);
  const [kpi, setKpi] = useState({ revenue: 0, purchases: 0 });

  useEffect(() => {
    fetchRealtime();
    fetchBatch();
    fetchKpi();

    const interval = setInterval(() => {
      fetchRealtime();
      fetchKpi();
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const fetchRealtime = async () => {
    const res = await axios.get("http://localhost:5000/api/country-agg");
    setRealtime(res.data);
  };

  const fetchBatch = async () => {
    const stateRes = await axios.get("http://localhost:5000/api/batch/state-revenue");
    const monthlyRes = await axios.get("http://localhost:5000/api/batch/monthly-revenue");
    setBatchState(stateRes.data);
    setBatchMonthly(monthlyRes.data);
  };

  const fetchKpi = async () => {
    const res = await axios.get("http://localhost:5000/api/kpi");
    setKpi(res.data || { revenue: 0, purchases: 0 });
  };

  return (
    <div style={{
      backgroundColor: "#111",
      color: "#fff",
      padding: "30px",
      fontFamily: "Arial",
      minHeight: "100vh"
    }}>
      <h1>E-Commerce Analytics Platform</h1>

      {/* REAL-TIME SECTION */}
      <h2 style={{ marginTop: "40px" }}>🔴 Real-Time Analytics</h2>

      <div style={{ display: "flex", gap: "40px", marginBottom: "30px" }}>
        <div style={{ background: "#1f1f1f", padding: "20px", borderRadius: "8px" }}>
          <h3>Total Revenue</h3>
          <h2>₹ {kpi?.revenue || 0}</h2>
        </div>

        <div style={{ background: "#1f1f1f", padding: "20px", borderRadius: "8px" }}>
          <h3>Total Purchases</h3>
          <h2>{kpi?.purchases || 0}</h2>
        </div>
      </div>

      {realtime.length > 0 && (
        <Bar
          data={{
            labels: realtime.map(r => r.country),
            datasets: [{
              label: "Live Revenue",
              data: realtime.map(r => r.total_revenue),
              backgroundColor: "rgba(255,99,132,0.6)"
            }]
          }}
        />
      )}

      {/* BATCH SECTION */}
      <h2 style={{ marginTop: "60px" }}>🔵 Batch Analytics (Historical)</h2>

      {batchState.length > 0 && (
        <Bar
          data={{
            labels: batchState.map(r => r.customer_state),
            datasets: [{
              label: "State Revenue (Historical)",
              data: batchState.map(r => r.total_revenue),
              backgroundColor: "rgba(54,162,235,0.6)"
            }]
          }}
        />
      )}

      {batchMonthly.length > 0 && (
        <Line
          data={{
            labels: batchMonthly.map(r => r.month),
            datasets: [{
              label: "Monthly Revenue",
              data: batchMonthly.map(r => r.total_revenue),
              borderColor: "rgba(75,192,192,1)",
              backgroundColor: "rgba(75,192,192,0.2)"
            }]
          }}
        />
      )}
    </div>
  );
}

export default App;