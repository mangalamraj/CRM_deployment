import mongoose, { ConnectOptions } from "mongoose";
import dotenv from "dotenv";

dotenv.config();

export const Connection = async () => {
  const URL = "mongodb+srv://mango:mango123@cluster0.nqhia7c.mongodb.net/";
  const options: ConnectOptions = {
    bufferCommands: false,
  };
  try {
    await mongoose.connect(
      "mongodb+srv://mango:mango123@cluster0.nqhia7c.mongodb.net/",
      options,
    );
    console.log("Database connected sucessfully");
  } catch (error: any) {
    console.log("error while connecting with the database", error.message);
  }
};
