import amqp from "amqplib";
import { Connection } from "./database/db";

interface Customer {
  custName: string;
  custEmail: string;
  status: string;
}
const EXCHANGE = "venderExchange";

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

          if (customer.status == "SENT") {
            console.log(`Email sent to ${customer.custName}`);
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
