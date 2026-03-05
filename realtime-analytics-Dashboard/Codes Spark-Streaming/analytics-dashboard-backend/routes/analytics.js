const express = require("express");
const router = express.Router();
const db = require("../db");

// Get country aggregation
router.get("/country-agg", (req, res) => {
  db.query("SELECT * FROM countryAgg", (err, results) => {
    if (err) return res.status(500).json(err);
    res.json(results);
  });
});

// Get KPI totals
router.get("/kpi", (req, res) => {
  db.query(
    "SELECT SUM(total_revenue) AS revenue, SUM(purchase_count) AS purchases FROM countryAgg",
    (err, results) => {
      if (err) return res.status(500).json(err);
      res.json(results[0]);
    }
  );
});

module.exports = router;