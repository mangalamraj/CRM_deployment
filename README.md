#  Mini CRM

Welcome to the  Mini CRM project! This project is a demonstration of a basic Customer Relationship Management (CRM) application developed using the following technologies:
- Next.js
- TypeScript
- MongoDB Atlas
- Firebase (Authentication)
- RabbitMQ Cloud (PubSub)
- Express.js
- ShadCN
- TailwindCSS

## Table of Contents
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the Application](#running-the-application)
- [Technologies Used](#technologies-used)
- [Contributing](#contributing)
- [License](#license)

## Features
- User authentication with Firebase
- CRUD operations for customer data
- Real-time updates and notifications using RabbitMQ Cloud
- Responsive UI with TailwindCSS
- Organized and modular codebase with TypeScript

## Getting Started

### Prerequisites
Make sure you have the following installed on your machine:
- Node.js (v14 or higher)
- pnpm (v6 or higher)
- MongoDB Atlas account
- RabbitMQ Cloud account

### Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/mangalamraj/CRM.git
    cd CRM
    ```

2. Install dependencies for both the client and server:
    ```bash
    cd client
    pnpm i
    cd ../server
    pnpm i
    ```

### Running the Application
To start the application, you need to run both the client and the server.

1. Start the client:
    ```bash
    cd client
    pnpm run dev
    ```

2. Start the server:
    ```bash
    cd server
    pnpm run develop & pnpm run start:all
    ```

Now, the application should be running, and you can access it in your web browser.

## Technologies Used

### Frontend
- **Next.js**: A React framework for server-side rendering and building static web applications.
- **TypeScript**: A strongly typed programming language that builds on JavaScript.
- **ShadCN**: A design system built with React and TailwindCSS.
- **TailwindCSS**: A utility-first CSS framework.

### Backend
- **Express.js**: A web application framework for Node.js.
- **MongoDB Atlas**: A cloud-based NoSQL database for storing customer data.
- **Firebase**: A platform for web and mobile development used here for authentication.
- **RabbitMQ Cloud**: A cloud-based message broker for handling real-time updates and notifications.

## License
This project is made by Mangalam Raj
