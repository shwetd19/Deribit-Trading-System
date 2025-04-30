#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <queue>
#include <functional>
#include <atomic>
#include <memory>
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/websocket.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>

// Using namespaces for brevity
using namespace boost::asio;
using namespace boost::beast;
using json = nlohmann::json;

// Configuration constants
const std::string DERIBIT_TEST_WS = "wss://test.deribit.com/ws/api/v2";
const std::string LOG_FILE = "deribit_trading.log";
const size_t MAX_LOG_SIZE = 1048576 * 5; // 5MB
const int MAX_LOG_FILES = 3;

// Performance metrics
struct PerformanceMetrics {
    std::atomic<double> order_placement_latency{0.0};
    std::atomic<double> market_data_latency{0.0};
    std::atomic<double> websocket_propagation_delay{0.0};
    std::atomic<double> end_to_end_latency{0.0};
};

// WebSocket Session
class WebSocketSession : public std::enable_shared_from_this<WebSocketSession> {
public:
    WebSocketSession(io_context& ioc, ssl::context& ssl_ctx)
        : ws_(ioc, ssl_ctx), resolver_(ioc), strand_(ioc) {}

    void connect(const std::string& host, const std::string& port) {
        resolver_.async_resolve(host, port,
            [self = shared_from_this()](const error_code& ec, ip::tcp::resolver::results_type results) {
                if (!ec) self->on_resolve(results);
                else spdlog::error("Resolve failed: {}", ec.message());
            });
    }

    void subscribe(const std::string& symbol) {
        subscribed_symbols_.insert(symbol);
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "public/subscribe"},
            {"params", {{"channels", {"book." + symbol + ".100ms"}}}},
            {"id", next_id_++}
        };
        write(msg.dump());
    }

    void write(const std::string& message) {
        auto self = shared_from_this();
        post(strand_, [self, message]() {
            self->outgoing_.push(message);
            if (self->outgoing_.size() == 1) self->do_write();
        });
    }

private:
    void on_resolve(ip::tcp::resolver::results_type results) {
        auto self = shared_from_this();
        boost::asio::async_connect(ws_.next_layer().next_layer(), results.begin(), results.end(),
            [self](const error_code& ec, auto) {
                if (!ec) self->ws_.next_layer().async_handshake(ssl::stream_base::client,
                    [self](const error_code& ec) {
                        if (!ec) self->ws_.async_handshake(self->host_, "/",
                            [self](const error_code& ec) {
                                if (!ec) self->do_read();
                                else spdlog::error("Handshake failed: {}", ec.message());
                            });
                        else spdlog::error("SSL Handshake failed: {}", ec.message());
                    });
                else spdlog::error("Connect failed: {}", ec.message());
            });
    }

    void do_read() {
        auto self = shared_from_this();
        ws_.async_read(buffer_,
            [self](const error_code& ec, std::size_t) {
                if (!ec) {
                    auto data = buffers_to_string(self->buffer_.data());
                    self->buffer_.consume(self->buffer_.size());
                    self->handle_message(data);
                    self->do_read();
                } else {
                    spdlog::error("Read failed: {}", ec.message());
                }
            });
    }

    void do_write() {
        auto self = shared_from_this();
        if (outgoing_.empty()) return;
        ws_.async_write(buffer(outgoing_.front()),
            [self](const error_code& ec, std::size_t) {
                if (!ec) {
                    self->outgoing_.pop();
                    self->do_write();
                } else {
                    spdlog::error("Write failed: {}", ec.message());
                }
            });
    }

    void handle_message(const std::string& message) {
        try {
            auto j = json::parse(message);
            if (j.contains("params") && j["params"].contains("data")) {
                auto timestamp = std::chrono::high_resolution_clock::now();
                // Broadcast to subscribers
                for (const auto& client : clients_) {
                    client->write(message);
                }
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - timestamp).count();
                metrics_.market_data_latency = duration / 1000.0;
            }
        } catch (const std::exception& e) {
            spdlog::error("JSON parse error: {}", e.what());
        }
    }

    websocket::stream<ssl::stream<ip::tcp::socket>> ws_;
    ip::tcp::resolver resolver_;
    io_context::strand strand_;
    flat_buffer buffer_;
    std::queue<std::string> outgoing_;
    std::string host_ = "test.deribit.com";
    std::unordered_set<std::string> subscribed_symbols_;
    std::vector<std::shared_ptr<WebSocketSession>> clients_;
    PerformanceMetrics& metrics_ = PerformanceMetricsSingleton::instance();
    int next_id_ = 1;
};

// Deribit API Client
class DeribitClient {
public:
    DeribitClient(io_context& ioc, ssl::context& ssl_ctx, const std::string& client_id, const std::string& client_secret)
        : ws_(std::make_shared<WebSocketSession>(ioc, ssl_ctx)),
          client_id_(client_id), client_secret_(client_secret) {}

    void start() {
        ws_->connect("test.deribit.com", "443");
        authenticate();
    }

    void place_order(const std::string& instrument, const std::string& type, double quantity, double price) {
        auto start = std::chrono::high_resolution_clock::now();
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "private/buy"},
            {"params", {
                {"instrument_name", instrument},
                {"amount", quantity},
                {"type", type},
                {"price", price}
            }},
            {"id", next_id_++}
        };
        ws_->write(msg.dump());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - start).count();
        metrics_.order_placement_latency = duration / 1000.0;
    }

    void cancel_order(const std::string& order_id) {
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "private/cancel"},
            {"params", {{"order_id", order_id}}},
            {"id", next_id_++}
        };
        ws_->write(msg.dump());
    }

    void modify_order(const std::string& order_id, double quantity, double price) {
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "private/edit"},
            {"params", {
                {"order_id", order_id},
                {"amount", quantity},
                {"price", price}
            }},
            {"id", next_id_++}
        };
        ws_->write(msg.dump());
    }

    void get_orderbook(const std::string& instrument) {
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "public/get_order_book"},
            {"params", {{"instrument_name", instrument}}},
            {"id", next_id_++}
        };
        ws_->write(msg.dump());
    }

    void get_positions() {
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "private/get_positions"},
            {"params", {{"currency", "BTC"}}},
            {"id", next_id_++}
        };
        ws_->write(msg.dump());
    }

    void subscribe_symbol(const std::string& symbol) {
        ws_->subscribe(symbol);
    }

private:
    void authenticate() {
        json msg = {
            {"jsonrpc", "2.0"},
            {"method", "public/auth"},
            {"params", {
                {"grant_type", "client_credentials"},
                {"client_id", client_id_},
                {"client_secret", client_secret_}
            }},
            {"id", next_id_++}
        };
        ws_->write(msg.dump());
    }

    std::shared_ptr<WebSocketSession> ws_;
    std::string client_id_;
    std::string client_secret_;
    PerformanceMetrics& metrics_ = PerformanceMetricsSingleton::instance();
    int next_id_ = 1;
};

// Singleton for performance metrics
class PerformanceMetricsSingleton {
public:
    static PerformanceMetrics& instance() {
        static PerformanceMetrics metrics;
        return metrics;
    }
};

// WebSocket Server for clients
class WebSocketServer {
public:
    WebSocketServer(io_context& ioc, unsigned short port)
        : acceptor_(ioc, ip::tcp::endpoint(ip::tcp::v4(), port)) {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](const error_code& ec, ip::tcp::socket socket) {
                if (!ec) {
                    auto session = std::make_shared<WebSocketSession>(socket.get_executor().context(), ssl_ctx_);
                    sessions_.push_back(session);
                    session->connect("localhost", std::to_string(acceptor_.local_endpoint().port()));
                }
                do_accept();
            });
    }

    ip::tcp::acceptor acceptor_;
    ssl::context ssl_ctx_{ssl::context::tlsv13};
    std::vector<std::shared_ptr<WebSocketSession>> sessions_;
};

// Main application
int main() {
    try {
        // Initialize logging
        auto logger = spdlog::rotating_logger_mt("deribit", LOG_FILE, MAX_LOG_SIZE, MAX_LOG_FILES);
        spdlog::set_default_logger(logger);
        spdlog::set_level(spdlog::level::info);

        io_context ioc;
        ssl::context ssl_ctx{ssl::context::tlsv13_client};
        ssl_ctx.set_verify_mode(ssl::verify_none); // For test environment

        // Replace with your Deribit Test API credentials
        std::string client_id = "YOUR_CLIENT_ID";
        std::string client_secret = "YOUR_CLIENT_SECRET";

        DeribitClient client(ioc, ssl_ctx, client_id, client_secret);
        WebSocketServer server(ioc, 8080);

        client.start();

        // Example usage
        client.subscribe_symbol("BTC-PERPETUAL");
        client.place_order("BTC-PERPETUAL", "limit", 1.0, 50000.0);
        client.get_orderbook("BTC-PERPETUAL");
        client.get_positions();

        // Run event loop
        std::vector<std::thread> threads;
        for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
            threads.emplace_back([&ioc]() { ioc.run(); });
        }
        for (auto& t : threads) {
            t.join();
        }
    } catch (const std::exception& e) {
        spdlog::error("Main error: {}", e.what());
    }

    return 0;
}
