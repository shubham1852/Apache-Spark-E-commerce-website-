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

  /* ---------- FETCH FUNCTIONS ---------- */

  const fetchRealtime = async () => {
    try {
      const res = await axios.get("http://localhost:5000/api/country-agg");
      setRealtime(res.data || []);
    } catch (err) {
      console.error(err);
    }
  };

  const fetchBatch = async () => {
    try {
      const stateRes = await axios.get("http://localhost:5000/api/batch/state-revenue");
      const monthlyRes = await axios.get("http://localhost:5000/api/batch/monthly-revenue");

      setBatchState(stateRes.data || []);
      setBatchMonthly(monthlyRes.data || []);
    } catch (err) {
      console.error(err);
    }
  };

  const fetchKpi = async () => {
    try {
      const res = await axios.get("http://localhost:5000/api/kpi");
      setKpi(res.data || { revenue: 0, purchases: 0 });
    } catch (err) {
      console.error(err);
    }
  };

  /* ---------- AUTO REFRESH ---------- */

  useEffect(() => {

    fetchRealtime();
    fetchBatch();
    fetchKpi();

    const interval = setInterval(() => {
      fetchRealtime();
      fetchKpi();
    }, 3000);

    return () => clearInterval(interval);

  }, []);

  /* ---------- CHART DATA ---------- */

  const realtimeChart = {
    labels: realtime.map(r => r.country),
    datasets: [
      {
        label: "Live Revenue",
        data: realtime.map(r => r.total_revenue),
        backgroundColor: "rgba(255,99,132,0.6)"
      }
    ]
  };

  const stateChart = {
    labels: batchState.map(r => r.customer_state),
    datasets: [
      {
        label: "State Revenue (Historical)",
        data: batchState.map(r => r.total_revenue),
        backgroundColor: "rgba(54,162,235,0.6)"
      }
    ]
  };

  const monthlyChart = {
    labels: batchMonthly.map(r => r.month),
    datasets: [
      {
        label: "Monthly Revenue",
        data: batchMonthly.map(r => r.total_revenue),
        borderColor: "rgba(75,192,192,1)",
        backgroundColor: "rgba(75,192,192,0.2)"
      }
    ]
  };

  /* ---------- UI ---------- */

  return (
    <div style={{
      backgroundColor: "#0f0f0f",
      color: "#fff",
      minHeight: "100vh",
      width: "100vw",
      padding: "40px",
      boxSizing: "border-box",
      fontFamily: "Arial"
    }}>

      {/* HEADER */}
      <h1 style={{ fontSize: "36px", marginBottom: "40px" }}>
        E-Commerce Analytics Platform
      </h1>

      {/* REALTIME SECTION */}
      <h2 style={{ marginBottom: "20px" }}>🔴 Real-Time Analytics</h2>

      <div style={{
        display: "flex",
        gap: "30px",
        marginBottom: "40px"
      }}>

        <div style={{
          background: "#1c1c1c",
          padding: "25px",
          borderRadius: "10px",
          width: "250px"
        }}>
          <h3>Total Revenue</h3>
          <h1>₹ {kpi.revenue}</h1>
        </div>

        <div style={{
          background: "#1c1c1c",
          padding: "25px",
          borderRadius: "10px",
          width: "250px"
        }}>
          <h3>Total Purchases</h3>
          <h1>{kpi.purchases}</h1>
        </div>

      </div>

      {realtime.length > 0 && (
        <div style={{
          width: "100%",
          background: "#1c1c1c",
          padding: "30px",
          borderRadius: "10px",
          marginBottom: "60px"
        }}>
          <Bar data={realtimeChart}/>
        </div>
      )}

      {/* BATCH SECTION */}
      <h2 style={{ marginBottom: "30px" }}>
        🔵 Batch Analytics (Historical)
      </h2>

      <div style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr",
        gap: "30px"
      }}>

        {batchState.length > 0 && (
          <div style={{
            background: "#1c1c1c",
            padding: "30px",
            borderRadius: "10px"
          }}>
            <Bar data={stateChart}/>
          </div>
        )}

        {batchMonthly.length > 0 && (
          <div style={{
            background: "#1c1c1c",
            padding: "30px",
            borderRadius: "10px"
          }}>
            <Line data={monthlyChart}/>
          </div>
        )}

      </div>

    </div>
  );
}

export default App;