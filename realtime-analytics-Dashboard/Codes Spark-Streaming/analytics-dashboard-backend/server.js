const express = require("express");
const cors = require("cors");
const mysql = require("mysql2");

const batchRoutes = require("./routes/batchRoutes");

const app = express();
app.use(cors());
app.use(express.json());

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "example",
  database: "users"
});

/* ---------------- BATCH ROUTES ---------------- */
app.use("/api/batch", batchRoutes);

/* ---------------- REALTIME ROUTE ---------------- */
app.get("/api/country-agg", (req, res) => {

  const query =
    "SELECT country, purchase_count, total_revenue FROM countryAgg";

  connection.query(query, (err, results) => {
    if (err) {
      console.error(err);
      return res.status(500).json(err);
    }

    res.json(results);
  });
});

/* ---------------- KPI ROUTE ---------------- */
app.get("/api/kpi", (req, res) => {

  const query =
    "SELECT SUM(total_revenue) as revenue, SUM(purchase_count) as purchases FROM countryAgg";

  connection.query(query, (err, results) => {
    if (err) return res.status(500).json(err);

    res.json(results[0]);
  });
});

app.listen(5000, () => {
  console.log("Server running on port 5000");
});