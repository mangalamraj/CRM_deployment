import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import bodyParser from "body-parser";
import { Connection } from "./database/db";
import Router from "./routes/route";

const app = express();
dotenv.config;
app.use(
  cors({
    origin: ["https://crm-deployment.vercel.app"],
    methods: ["POST", "GET"],
    credentials: true,
  }),
);

app.get("/", (req: any, res: any) => {
  res.json("Hello");
});
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use("/", Router);
Connection();
app.listen(8000, () => console.log("server is listening on port 8000."));
