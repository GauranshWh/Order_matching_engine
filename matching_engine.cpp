#include <map>
#include <vector>
#include <algorithm>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <boost/lockfree/stack.hpp>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <sched.h>
#include <csignal>
#include <unordered_set>

enum class Side { Buy, Sell };

struct alignas(64) Order {
    uint64_t order_id;
    Side side;
    double price;
    uint32_t quantity;
    std::chrono::nanoseconds timestamp;
};

struct Trade {
    uint64_t buy_order_id;
    uint64_t sell_order_id;
    double price;
    uint32_t quantity;
    std::chrono::nanoseconds timestamp;
};

std::atomic<bool> disable_logging{false}; // Control logging for performance

class OrderPool {
private:
    struct Node {
        Order order;
        std::atomic<Node*> next{nullptr};
    };
    std::vector<Node> pool;
    boost::lockfree::stack<Node*> free_list;
    std::atomic<size_t> allocated_count{0};
    mutable std::mutex alloc_mutex;
    std::unordered_set<Order*> allocated_nodes;

public:
    OrderPool(size_t size) : pool(size), free_list(size) {
        if (!disable_logging) std::cout << "Initializing OrderPool with size " << size << "\n";
        for (auto& node : pool) {
            free_list.push(&node);
        }
    }
    Order* allocate() {
        Node* node = nullptr;
        if (free_list.pop(node)) {
            std::lock_guard<std::mutex> lock(alloc_mutex);
            node->order.order_id = 0;
            allocated_count.fetch_add(1, std::memory_order_relaxed);
            allocated_nodes.insert(&node->order);
            if (!disable_logging) std::cout << "Allocated order at " << &node->order << "\n";
            return &node->order;
        }
        if (!disable_logging) std::cerr << "Error: Order pool exhausted\n";
        return nullptr;
    }
    void deallocate(Order* order) {
        if (!order) {
            if (!disable_logging) std::cerr << "Error: Null order in deallocate\n";
            return;
        }
        std::lock_guard<std::mutex> lock(alloc_mutex);
        if (allocated_nodes.find(order) == allocated_nodes.end()) {
            if (!disable_logging) std::cerr << "Error: Attempt to deallocate unallocated order " << order << " (id=" << order->order_id << ")\n";
            return;
        }
        allocated_nodes.erase(order);
        Node* node = reinterpret_cast<Node*>(order);
        free_list.push(node);
        allocated_count.fetch_sub(1, std::memory_order_relaxed);
        if (!disable_logging) std::cout << "Deallocated order " << order << " (id=" << order->order_id << ")\n";
    }
    bool is_allocated(Order* order) const {
        if (!order) return false;
        std::lock_guard<std::mutex> lock(alloc_mutex);
        return allocated_nodes.find(order) != allocated_nodes.end();
    }
    size_t get_allocated_count() const { return allocated_count.load(std::memory_order_relaxed); }
};

class OrderQueue {
private:
    std::queue<Order*> queue;
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::atomic<bool> running{true};

public:
    OrderQueue() {}
    bool push(Order* order) {
        if (!order) {
            if (!disable_logging) std::cerr << "Error: Null order in push\n";
            return false;
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!disable_logging) {
                std::cout << "Pushing order " << order << " (id=" << order->order_id << ")\n";
                std::cout << "Pushed order to queue, size=" << queue.size() + 1 << "\n";
            }
            queue.push(order);
        }
        cv.notify_all();
        return true;
    }
    bool pop(Order*& order) {
        std::unique_lock<std::mutex> lock(mutex);
        if (!running && queue.empty()) {
            if (!disable_logging) std::cout << "Queue stopped, empty=1\n";
            return false;
        }
        bool result = cv.wait_for(lock, std::chrono::seconds(5), 
            [this] { return !queue.empty() || !running; });
        if (!result && queue.empty()) {
            if (!disable_logging) std::cout << "Pop timeout, queue empty=1, running=" << running << "\n";
            return false;
        }
        if (!running && queue.empty()) {
            if (!disable_logging) std::cout << "Queue stopped, empty=1\n";
            return false;
        }
        order = queue.front();
        queue.pop();
        if (!disable_logging) std::cout << "Popped order " << order << " from queue, size=" << queue.size() << "\n";
        return true;
    }
    void stop() {
        running = false;
        if (!disable_logging) std::cout << "Stopping OrderQueue\n";
        cv.notify_all();
    }
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex);
        return queue.size();
    }
};

class OrderBook {
private:
    struct PriceLevel {
        std::vector<Order*> orders;
    };
    std::map<double, PriceLevel, std::greater<double>> bids;
    std::map<double, PriceLevel, std::less<double>> asks;
    std::mutex mutex;
    const OrderPool& order_pool;

public:
    OrderBook(const OrderPool& pool) : order_pool(pool) {}
    void add_buy_order(Order* order) {
        if (!order || !order_pool.is_allocated(order)) {
            if (!disable_logging) std::cerr << "Error: Invalid or unallocated buy order " << order << "\n";
            return;
        }
        std::lock_guard<std::mutex> lock(mutex);
        bids[order->price].orders.push_back(order);
        if (!disable_logging) std::cout << "Added buy order at price=" << order->price << ", orders at level=" << bids[order->price].orders.size() << "\n";
    }
    void add_sell_order(Order* order) {
        if (!order || !order_pool.is_allocated(order)) {
            if (!disable_logging) std::cerr << "Error: Invalid or unallocated sell order " << order << "\n";
            return;
        }
        std::lock_guard<std::mutex> lock(mutex);
        asks[order->price].orders.push_back(order);
        if (!disable_logging) std::cout << "Added sell order at price=" << order->price << ", orders at level=" << asks[order->price].orders.size() << "\n";
    }
    void remove_buy_order(Order* order) {
        if (!order || !order_pool.is_allocated(order)) {
            if (!disable_logging) std::cerr << "Error: Invalid or unallocated buy order for removal " << order << "\n";
            return;
        }
        std::lock_guard<std::mutex> lock(mutex);
        auto it = bids.find(order->price);
        if (it != bids.end()) {
            auto& orders = it->second.orders;
            if (!disable_logging) std::cout << "Removing buy order " << order << " at price=" << order->price << ", current orders=" << orders.size() << "\n";
            orders.erase(std::remove(orders.begin(), orders.end(), order), orders.end());
            if (!disable_logging) std::cout << "Removed buy order at price=" << order->price << ", orders left=" << orders.size() << "\n";
            if (orders.empty()) bids.erase(it);
        }
    }
    void remove_sell_order(Order* order) {
        if (!order || !order_pool.is_allocated(order)) {
            if (!disable_logging) std::cerr << "Error: Invalid or unallocated sell order for removal " << order << "\n";
            return;
        }
        std::lock_guard<std::mutex> lock(mutex);
        auto it = asks.find(order->price);
        if (it != asks.end()) {
            auto& orders = it->second.orders;
            if (!disable_logging) std::cout << "Removing sell order " << order << " at price=" << order->price << ", current orders=" << orders.size() << "\n";
            orders.erase(std::remove(orders.begin(), orders.end(), order), orders.end());
            if (!disable_logging) std::cout << "Removed sell order at price=" << order->price << ", orders left=" << orders.size() << "\n";
            if (orders.empty()) asks.erase(it);
        }
    }
    std::pair<std::vector<Order*>, double> get_best_ask() {
        std::lock_guard<std::mutex> lock(mutex);
        if (asks.empty()) {
            if (!disable_logging) std::cout << "No asks available\n";
            return {{}, 0.0};
        }
        auto it = asks.begin();
        if (!disable_logging) std::cout << "Best ask price=" << it->first << ", orders=" << it->second.orders.size() << "\n";
        return {it->second.orders, it->first};
    }
    std::pair<std::vector<Order*>, double> get_best_bid() {
        std::lock_guard<std::mutex> lock(mutex);
        if (bids.empty()) {
            if (!disable_logging) std::cout << "No bids available\n";
            return {{}, 0.0};
        }
        auto it = bids.begin();
        if (!disable_logging) std::cout << "Best bid price=" << it->first << ", orders=" << it->second.orders.size() << "\n";
        return {it->second.orders, it->first};
    }
    std::pair<std::vector<Order*>, double> get_best_opposite(Side side) {
        return (side == Side::Buy) ? get_best_ask() : get_best_bid();
    }
    void clear() {
        std::lock_guard<std::mutex> lock(mutex);
        bids.clear();
        asks.clear();
        if (!disable_logging) std::cout << "Cleared OrderBook\n";
    }
};

class MatchingEngine {
private:
    OrderPool order_pool;
    OrderBook order_book;
    OrderQueue input_queue;
    std::vector<Trade> trades;
    std::vector<std::thread> workers;
    std::mutex trades_mutex;

    void worker_thread() {
        if (!disable_logging) std::cout << "Worker thread " << std::this_thread::get_id() << " started\n";
        Order* order;
        while (input_queue.pop(order)) {
            process_order(order);
        }
        if (!disable_logging) std::cout << "Worker thread " << std::this_thread::get_id() << " stopped\n";
    }

    void process_order(Order* order) {
        if (!order) {
            if (!disable_logging) std::cerr << "Error: Null order in process_order\n";
            return;
        }
        if (!disable_logging) std::cout << "Processing order " << order->order_id << " (" << (order->side == Side::Buy ? "Buy" : "Sell") << ", price=" << order->price << ", qty=" << order->quantity << ")\n";
        if (!order_pool.is_allocated(order)) {
            if (!disable_logging) std::cerr << "Error: Processing unallocated order " << order << " (id=" << order->order_id << ")\n";
            return;
        }
        if (order->side == Side::Buy) {
            order_book.add_buy_order(order);
        } else {
            order_book.add_sell_order(order);
        }
        while (order->quantity > 0) {
            auto [opposite_orders, price] = order_book.get_best_opposite(order->side);
            if (opposite_orders.empty() || 
                (order->side == Side::Buy && order->price < price) ||
                (order->side == Side::Sell && order->price > price)) {
                break;
            }
            Order* opp_order = opposite_orders.front();
            if (!opp_order) {
                if (!disable_logging) std::cerr << "Error: Null opposite order\n";
                break;
            }
            if (!order_pool.is_allocated(opp_order)) {
                if (!disable_logging) std::cerr << "Error: Opposite order " << opp_order << " (id=" << opp_order->order_id << ") is not allocated\n";
                break;
            }
            uint32_t match_qty = std::min(order->quantity, opp_order->quantity);
            if (match_qty > 0) { // Prevent zero-quantity trades
                std::lock_guard<std::mutex> lock(trades_mutex);
                uint64_t buy_id = (order->side == Side::Buy) ? order->order_id : opp_order->order_id;
                uint64_t sell_id = (order->side == Side::Sell) ? order->order_id : opp_order->order_id;
                trades.push_back({buy_id, sell_id, price, match_qty,
                                  std::chrono::system_clock::now().time_since_epoch()});
                if (!disable_logging) std::cout << "Trade: buy=" << buy_id << ", sell=" << sell_id << ", price=" << price << ", qty=" << match_qty << "\n";
            }
            order->quantity -= match_qty;
            opp_order->quantity -= match_qty;
            if (opp_order->quantity == 0) {
                if (opp_order->side == Side::Buy) {
                    order_book.remove_buy_order(opp_order);
                } else {
                    order_book.remove_sell_order(opp_order);
                }
                if (order_pool.is_allocated(opp_order)) {
                    order_pool.deallocate(opp_order);
                }
            }
        }
        if (order->quantity == 0) {
            if (order->side == Side::Buy) {
                order_book.remove_buy_order(order);
            } else {
                order_book.remove_sell_order(order);
            }
            if (order_pool.is_allocated(order)) {
                order_pool.deallocate(order);
            }
        }
    }

public:
    MatchingEngine(size_t pool_size, size_t num_threads = std::thread::hardware_concurrency())
        : order_pool(pool_size), order_book(order_pool) {
        if (!disable_logging) std::cout << "Initializing MatchingEngine with " << num_threads << " threads\n";
        trades.reserve(100'000);
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back(&MatchingEngine::worker_thread, this);
        }
    }
    ~MatchingEngine() {
        input_queue.stop();
        for (auto& worker : workers) {
            if (worker.joinable()) worker.join();
        }
        if (!disable_logging) std::cout << "MatchingEngine destroyed\n";
    }
    Order* allocate_order() {
        if (!disable_logging) std::cout << "Allocating order\n";
        Order* order = order_pool.allocate();
        if (!disable_logging) std::cout << "Order allocated: " << (order ? "success" : "failed") << "\n";
        return order;
    }
    void submit_order(Order* order) { 
        if (!order) {
            if (!disable_logging) std::cerr << "Error: Null order submitted\n";
            return;
        }
        if (!disable_logging) std::cout << "Submitting order " << order->order_id << " at " << order << "\n";
        input_queue.push(order);
        if (!disable_logging) std::cout << "Submitted order " << order->order_id << "\n";
    }
    const std::vector<Trade>& get_trades() const { return trades; }
    void clear() {
        std::lock_guard<std::mutex> lock(trades_mutex);
        trades.clear();
        order_book.clear();
        if (input_queue.size() > 0) {
            Order* order;
            while (input_queue.pop(order)) {
                if (order_pool.is_allocated(order)) {
                    order_pool.deallocate(order);
                }
            }
        }
        if (!disable_logging) std::cout << "Cleared MatchingEngine\n";
    }
    size_t get_queue_size() const { return input_queue.size(); }
    void stop() {
        input_queue.stop();
    }
};

MatchingEngine* global_engine = nullptr;

void signal_handler(int signum) {
    if (!disable_logging) std::cout << "Received signal " << signum << ", stopping engine\n";
    if (global_engine) {
        global_engine->stop();
    }
    exit(1);
}

void test_matching(MatchingEngine& engine) {
    std::cout << "Starting test_matching\n";
    engine.clear();
    std::cout << "After engine.clear in test_matching\n";
    Order* buy = engine.allocate_order();
    if (!buy) {
        std::cout << "Test failed: Cannot allocate buy order\n";
        return;
    }
    std::cout << "Buy order allocated, id=1\n";
    buy->order_id = 1;
    buy->side = Side::Buy;
    buy->price = 100.0;
    buy->quantity = 100;
    buy->timestamp = std::chrono::system_clock::now().time_since_epoch();
    engine.submit_order(buy);

    Order* sell = engine.allocate_order();
    if (!sell) {
        std::cout << "Test failed: Cannot allocate sell order\n";
        return;
    }
    std::cout << "Sell order allocated, id=2\n";
    sell->order_id = 2;
    sell->side = Side::Sell;
    sell->price = 100.0;
    sell->quantity = 100;
    sell->timestamp = std::chrono::system_clock::now().time_since_epoch();
    engine.submit_order(sell);

    while (engine.get_queue_size() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "Checking trades, queue size=" << engine.get_queue_size() << "\n";
    const auto& trades = engine.get_trades();
    if (trades.size() == 1 && trades[0].price == 100.0 && trades[0].quantity == 100) {
        std::cout << "Test passed: Buy and sell matched correctly\n";
    } else {
        std::cout << "Test failed: Expected 1 trade with price 100.0 and quantity 100, got " << trades.size() << " trades\n";
    }
}

void test_partial_match(MatchingEngine& engine) {
    std::cout << "Starting test_partial_match\n";
    engine.clear();
    std::cout << "After engine.clear in test_partial_match\n";
    Order* buy = engine.allocate_order();
    if (!buy) {
        std::cout << "Test failed: Cannot allocate buy order\n";
        return;
    }
    std::cout << "Buy order allocated, id=3\n";
    buy->order_id = 3;
    buy->side = Side::Buy;
    buy->price = 100.0;
    buy->quantity = 200;
    buy->timestamp = std::chrono::system_clock::now().time_since_epoch();
    engine.submit_order(buy);

    Order* sell = engine.allocate_order();
    if (!sell) {
        std::cout << "Test failed: Cannot allocate sell order\n";
        return;
    }
    std::cout << "Sell order allocated, id=4\n";
    sell->order_id = 4;
    sell->side = Side::Sell;
    sell->price = 100.0;
    sell->quantity = 100;
    sell->timestamp = std::chrono::system_clock::now().time_since_epoch();
    engine.submit_order(sell);

    while (engine.get_queue_size() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "Checking trades, queue size=" << engine.get_queue_size() << "\n";
    const auto& trades = engine.get_trades();
    if (trades.size() == 1 && trades[0].quantity == 100 && trades[0].price == 100.0 && buy->quantity == 100) {
        std::cout << "Test passed: Partial match correctly\n";
    } else {
        std::cout << "Test failed: Partial match, got " << trades.size() << " trades, buy quantity " << buy->quantity << "\n";
    }
}

void test_no_match(MatchingEngine& engine) {
    std::cout << "Starting test_no_match\n";
    engine.clear();
    std::cout << "After engine.clear in test_no_match\n";
    Order* buy = engine.allocate_order();
    if (!buy) {
        std::cout << "Test failed: Cannot allocate buy order\n";
        return;
    }
    std::cout << "Buy order allocated, id=5\n";
    buy->order_id = 5;
    buy->side = Side::Buy;
    buy->price = 90.0;
    buy->quantity = 100;
    buy->timestamp = std::chrono::system_clock::now().time_since_epoch();
    engine.submit_order(buy);

    Order* sell = engine.allocate_order();
    if (!sell) {
        std::cout << "Test failed: Cannot allocate sell order\n";
        return;
    }
    std::cout << "Sell order allocated, id=6\n";
    sell->order_id = 6;
    sell->side = Side::Sell;
    sell->price = 100.0;
    sell->quantity = 100;
    sell->timestamp = std::chrono::system_clock::now().time_since_epoch();
    engine.submit_order(sell);

    while (engine.get_queue_size() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "Checking trades, queue size=" << engine.get_queue_size() << "\n";
    const auto& trades = engine.get_trades();
    if (trades.empty()) {
        std::cout << "Test passed: No match due to price difference\n";
    } else {
        std::cout << "Test failed: Expected no trades, got " << trades.size() << " trades\n";
    }
}

void benchmark(MatchingEngine& engine, size_t num_orders) {
    std::cout << "Starting benchmark with " << num_orders << " orders\n";
    engine.clear();
    std::cout << "After engine.clear in benchmark\n";
    disable_logging = true; // Disable logging for performance
    auto start = std::chrono::high_resolution_clock::now();

    // Parallel order submission
    size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> submitters;
    size_t orders_per_thread = num_orders / num_threads;
    for (size_t t = 0; t < num_threads; ++t) {
        submitters.emplace_back([&engine, t, orders_per_thread, num_orders, num_threads]() {
            size_t start_id = t * orders_per_thread;
            size_t end_id = (t == num_threads - 1) ? num_orders : start_id + orders_per_thread;
            for (size_t i = start_id; i < end_id; ++i) {
                Order* order = engine.allocate_order();
                if (!order) {
                    if (!disable_logging) std::cerr << "Benchmark failed: Order pool exhausted at order " << i << "\n";
                    return;
                }
                order->order_id = i;
                order->side = (i % 2 == 0) ? Side::Buy : Side::Sell;
                order->price = 100.0 + (i % 10) * 0.1;
                order->quantity = 100;
                order->timestamp = std::chrono::system_clock::now().time_since_epoch();
                engine.submit_order(order);
            }
        });
    }
    for (auto& t : submitters) {
        if (t.joinable()) t.join();
    }

    // Wait for queue to drain
    while (engine.get_queue_size() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto end = std::chrono::high_resolution_clock::now();
    disable_logging = false; // Re-enable logging
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    std::cout << std::dec;
    std::cout << "Processed " << num_orders << " orders in " << duration / 1000.0 << " μs\n";
    std::cout << "Latency per order: " << duration / num_orders << " ns\n";
    std::cout << "Throughput: " << (num_orders * 1e9) / duration << " orders/sec\n";
    std::cout << "Trades generated: " << engine.get_trades().size() << "\n";
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();
    std::cout << "Starting main\n";
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (unsigned int i = 0; i < std::thread::hardware_concurrency(); ++i) {
        CPU_SET(i, &cpuset);
    }
    if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Failed to set CPU affinity\n";
    }
    MatchingEngine engine(1'000'000, 4);
    global_engine = &engine;
    std::signal(SIGINT, signal_handler);
    test_matching(engine);
    test_partial_match(engine);
    test_no_match(engine);
    benchmark(engine, 10'000); // Increased from 100 to 10,000
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << std::dec << "Total runtime: " << duration << " ms\n";
    return 0;
}