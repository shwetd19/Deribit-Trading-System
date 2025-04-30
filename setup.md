## Deribit Trading System

A high-performance C++20 trading application for the Deribit Testnet ([https://test.deribit.com](https://test.deribit.com)).  
This system handles order execution, management, order book retrieval, position tracking, and real-time market data via WebSocket.

---

### ✅ Features

- Place, cancel, and modify orders  
- Fetch live order books and open positions  
- Stream real-time market data via WebSocket  
- Built-in logging and error tracking  
- Lightweight and fast (optimized with C++20)

---

### 🧰 Prerequisites

- **C++20 compatible compiler** (e.g., GCC 11+, Clang 14+)  
- **Dependencies** (installable via `vcpkg` or your system package manager):
  - `boost-asio`
  - `nlohmann-json`
  - `spdlog`
- **Deribit Testnet Account + API Keys**  
  Generate keys at: [https://test.deribit.com](https://test.deribit.com)

---

### ⚙️ Setup

1. **Clone the repository**  
   ```bash
   git clone <repository-url>
   cd deribit-trading-system
   ```

2. **Install dependencies**  
   ```bash
   vcpkg install boost-asio nlohmann-json spdlog
   ```

3. **Configure API credentials in `main.cpp`**  
   ```cpp
   std::string client_id = "YOUR_CLIENT_ID";
   std::string client_secret = "YOUR_CLIENT_SECRET";
   ```

---

### 🛠️ Build

Compile the application with optimization flags:  
```bash
g++ -O3 -march=native -std=c++20 main.cpp -o deribit_trading \
    -I/path/to/vcpkg/installed/x64-linux/include \
    -L/path/to/vcpkg/installed/x64-linux/lib \
    -lboost_system -lssl -lcrypto -pthread
```

---

### ▶️ Run

Start the trading system:  
```bash
./deribit_trading
```

- Connects to Deribit Testnet WebSocket API  
- Launches a local WebSocket server on port `8080`  
- Example operations (e.g., order placement, data retrieval) are included in `main.cpp`

---

### 📄 Logging

- Logs are saved to `deribit_trading.log`  
- Rotates at 5MB (up to 3 backup files)  
- Useful for debugging and performance monitoring

---

### 📌 Notes

- Ensure a stable internet connection for uninterrupted WebSocket communication  
- Refer to `Performance_Analysis_Report.md` for benchmarking and tuning insights

---
