console.log("SERVER.TS IS RUNNING");
import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import { fileURLToPath } from "url";
import { Server } from "socket.io";
import { createServer } from "http";

process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// In-memory store for shared state
let state = {
  clients: [] as any[],
  products: [] as any[],
  orders: [] as any[]
};

async function startServer() {
  const app = express();
  const httpServer = createServer(app);
  const io = new Server(httpServer, {
    cors: {
      origin: "*",
      methods: ["GET", "POST"]
    }
  });

  const PORT = 3000;

  app.use(express.json());

  // API routes
  app.get("/api/health", (req, res) => {
    res.json({ status: "ok" });
  });

  app.get("/api/state", (req, res) => {
    res.json(state);
  });

  // Socket.io logic
  io.on("connection", (socket) => {
    console.log("A user connected:", socket.id);
    
    // Send initial state to the new client
    socket.emit("init", state);

    socket.on("update_clients", (clients) => {
      state.clients = clients;
      socket.broadcast.emit("clients_updated", clients);
    });

    socket.on("update_products", (products) => {
      state.products = products;
      socket.broadcast.emit("products_updated", products);
    });

    socket.on("update_orders", (orders) => {
      state.orders = orders;
      socket.broadcast.emit("orders_updated", orders);
    });

    socket.on("new_order", (order) => {
      state.orders.push(order);
      io.emit("orders_updated", state.orders);
    });

    socket.on("update_order_status", ({ orderId, status }) => {
      state.orders = state.orders.map(o => o.id === orderId ? { ...o, status } : o);
      io.emit("orders_updated", state.orders);
    });

    socket.on("disconnect", () => {
      console.log("User disconnected:", socket.id);
    });
  });

  console.log("Starting server in", process.env.NODE_ENV || "development", "mode");

  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://0.0.0.0:${PORT}`);
    console.log("Vite middleware should be active if not in production");
  });
}

startServer().catch(err => {
  console.error("Failed to start server:", err);
});
