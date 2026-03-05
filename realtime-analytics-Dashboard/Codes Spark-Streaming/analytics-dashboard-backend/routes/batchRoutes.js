const express = require("express");
const router = express.Router();
const mysql = require("mysql2");

// MySQL Connection
const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "example",
  database: "users"
});

// -----------------------------
// Get Batch Revenue by State
// -----------------------------
router.get("/state-revenue", (req, res) => {
  const query = "SELECT * FROM batch_state_revenue";

  connection.query(query, (err, results) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Database error" });
    }
    res.json(results);
  });
});

// -----------------------------
// Get Batch Monthly Revenue
// -----------------------------
router.get("/monthly-revenue", (req, res) => {
  const query = "SELECT * FROM batch_monthly_revenue";

  connection.query(query, (err, results) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ error: "Database error" });
    }
    res.json(results);
  });
});

module.exports = router;