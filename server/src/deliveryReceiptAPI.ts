import amqp from "amqplib";
import CommunicationLog from "./model/campaign-shema"; // Update path if necessary
import { Connection } from "./database/db"; // Update path if necessary

interface Customer {
  custName: string;
  custEmail: string;
  status: string;
}

const EXCHANGE = "campaignExchange";
const OUTPUT_EXCHANGE = "venderExchange";
Connection();

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(
      process.env.RABBITMQ_URL ? process.env.RABBITMQ_URL : "",
    );
    const channel = await connection.createChannel();

    await channel.assertExchange(EXCHANGE, "fanout", { durable: false });

    const queue = await channel.assertQueue("", { exclusive: true });

    await channel.bindQueue(queue.queue, EXCHANGE, "");

    console.log(
      `Consumer bound to exchange. Listening on queue: ${queue.queue}`,
    );

    channel.consume(queue.queue, async (msg) => {
      if (msg) {
        try {
          const customer: Customer = JSON.parse(msg.content.toString());
          console.log(`Received message: ${msg.content.toString()}`);
          await new Promise((resolve) => setTimeout(resolve, 2000));
          const statuses: "SENT" | "FAILED" =
            Math.random() < 0.9 ? "SENT" : "FAILED";

          const updatedLog = await CommunicationLog.findOneAndUpdate(
            {
              custName: customer.custName,
              custEmail: customer.custEmail,
              status: "PENDING",
            },
            { status: statuses },
            { new: true },
          );
          const message = JSON.stringify(updatedLog);

          channel.publish(OUTPUT_EXCHANGE, "", Buffer.from(message));

          console.log(updatedLog);
          if (updatedLog) {
            console.log("Status updated successfully:", updatedLog);
          } else {
            console.log("No matching document found to update.");
          }

          channel.ack(msg);
        } catch (error) {
          console.error("Error processing message:", error);

          channel.ack(msg);
        }
      }
    });

    console.log("RabbitMQ consumer connected and awaiting messages.");
  } catch (err) {
    console.error("Failed to connect to RabbitMQ:", err);
  }
}

connectRabbitMQ();
