import express from "express";
import {
  addShop,
  getAllOrderData,
  getCampaignData,
  getCustData,
  getShopData,
} from "../controller/shop-controller";
import amqp from "amqplib";

import Order from "../model/order-schema";
import Customer from "../model/customer-schema";
import CommunicationLog from "../model/campaign-shema";
import dotenv from "dotenv";
import axios from "axios";
import mongoose from "mongoose";
import { Connection } from "../database/db";
const connectToDB = Connection();
dotenv.config();

const router = express.Router();

let channel: amqp.Channel, connection: amqp.Connection;

const QUEUE = "campaignQueue";

// Define your routes here
router.post("/addshop", addShop);
router.post("/getshopdata", getShopData);
router.post("/getAllOrderData", getAllOrderData);
router.post("/getAllCustomerData", getCustData);
router.get("/getAllCampaignData", getCampaignData);

// Customer creation and queuing
router.post("/customer", async (req: any, res: any) => {
  const { custName, custEmail, spends, visits, lastVisits, shopName } =
    req.body;

  const customer = new Customer({
    custName,
    custEmail,
    spends,
    visits,
    lastVisits,
    shopName,
  });

  try {
    await customer.save();
    channel.sendToQueue("customers", Buffer.from(JSON.stringify(req.body)));
    res.status(201).send("Customer submitted and saved to DB");
  } catch (error) {
    res.status(500).send("Error submitting customer");
  }
});

// Campaign sending
router.post("/sendCampaign", async (req: any, res: any) => {
  const { customers } = req.body;

  if (!customers || !Array.isArray(customers)) {
    return res.status(400).send("Invalid input");
  }

  for (const customer of customers) {
    await channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(customer)));
  }

  res.status(200).send("Campaign sent to queue");
});

// Order creation and queuing
router.post("/order", async (req: any, res: any) => {
  const { orderName, orderEmail, amount, orderDate, shopName } = req.body;

  const order = new Order({
    orderName,
    orderEmail,
    amount,
    orderDate,
    shopName,
  });

  try {
    await order.save();
    channel.sendToQueue("orders", Buffer.from(JSON.stringify(req.body)));
    res.status(201).send("Order submitted and saved to DB");
  } catch (error) {
    res.status(500).send("Error submitting order");
  }
});

// Dummy Vendor API
router.post("/dummyVendorAPI/batch", (req: any, res: any) => {
  const messages = req.body.messages;
  // Handle the batch processing logic here
  console.log("Received batch messages:", messages);
  res.status(200).send("Batch processed successfully");
});

// Delivery Receipt API
router.post("/deliveryReceipt", async (req: any, res: any) => {
  await connectToDB;
  const { logId, status } = req.body;

  try {
    await CommunicationLog.findByIdAndUpdate(logId, { status });
    res.status(200).send("Status updated");
  } catch (error) {
    res.status(500).send("Error updating status");
  }
});

// Consumer setup for RabbitMQ
async function setupConsumer() {
  const BATCH_SIZE = 2;
  const BATCH_INTERVAL = 1000;
  let messageBuffer: amqp.Message[] = [];

  async function processBatch() {
    try {
      await connectToDB;
      const batch = messageBuffer.splice(0, BATCH_SIZE);
      if (batch.length === 0) return;

      for (const msg of batch) {
        const customer = JSON.parse(msg.content.toString());
        const log = new CommunicationLog({
          custName: customer.custName,
          custEmail: customer.custEmail,
          status: "PENDING",
        });
        await log.save();
      }

      await axios.post(
        "https://crm-deployment-server.vercel.app/dummyVendorAPI/batch",
        {
          messages: batch.map((msg) => JSON.parse(msg.content.toString())),
        },
      );

      const statuses: ("SENT" | "FAILED")[] = batch.map(() =>
        Math.random() < 0.9 ? "SENT" : "FAILED",
      );

      for (let i = 0; i < batch.length; i++) {
        const logId = (await CommunicationLog.findOne().sort({ _id: -1 }))!._id;
        await axios.post(
          "https://crm-deployment-server.vercel.app/deliveryReceipt",
          {
            logId,
            status: statuses[i],
          },
        );
      }
    } catch (error) {
      console.error("Error processing batch", error);
    }
  }

  setInterval(processBatch, BATCH_INTERVAL);

  channel.consume(QUEUE, async (msg: amqp.Message | null) => {
    if (msg !== null) {
      messageBuffer.push(msg);
      if (messageBuffer.length >= BATCH_SIZE) {
        await processBatch();
      }
      channel.ack(msg);
    }
  });
}

// RabbitMQ connection and channel setup
async function connectRabbitMQ() {
  try {
    connection = await amqp.connect(
      process.env.RABBITMQ_URL ||
        "amqps://lbgyymhn:SV9-imIoV_108rlH_nLajN9pwQ-DSFml@rattlesnake.rmq.cloudamqp.com/lbgyymhn",
    );
    channel = await connection.createChannel();
    await channel.assertQueue("orders");
    await channel.assertQueue("customers");
    await channel.assertQueue(QUEUE);
    console.log("Connected to RabbitMQ");

    // Once connected, setup the consumer
    await setupConsumer();
  } catch (error) {
    console.error("Failed to connect to RabbitMQ", error);
  }
}

connectRabbitMQ();

export default router;
