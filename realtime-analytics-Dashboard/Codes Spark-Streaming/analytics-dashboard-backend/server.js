const express = require("express");
const cors = require("cors");

const batchRoutes = require("./routes/batchRoutes");

const app = express();

app.use(cors());
app.use(express.json());

// Batch APIs
app.use("/api/batch", batchRoutes);

const PORT = 5000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});