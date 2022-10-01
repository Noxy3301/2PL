#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

#define PAGE_SIZE 4096
#define TUPLE_NUM 1000000
#define THREAD_NUM 12 //自分のパソコンのThread数

// commitしたものを数える用？
// わざわざstruct(class)にしているのは可読性を上げるため？
struct Result {
    public:
        uint64_t commit_cnt;
};

std::vector<Result> AllResult(THREAD_NUM);

enum struct Operation {
    READ,
    WRITE,
    SLEEP
};

struct Task {
    Operation ope;
    uint64_t key;

    // Taskのコンストラクタ(初期化みたいなやつ)、もしかしてここで名前が被るのが嫌だからStructの変数の末尾に_がついていたのか？
    Task(Operation ope, uint64_t key) : ope(ope), key(key) {};
};

struct RWLock {
    public:
        std::atomic<int> counter;
        // counter == -1 : write locked
        // coutner ==  0 : not locked
        // counter  >  0 : read locked by {counter} reader(s).

        RWLock() { counter.store(0, std::memory_order_release); }   
        // memory_order_release -> memory_order_acquireの順番を保証するメモリフェンス
        // これはcounterのinitialize, atomic系はstoreでwrite、loadでreadするらしい

        // CASで更新をAtomicに行う、expectedなら、desiredに書き換えるみたいな感じ
        // strongとweakの違いは、weakだとCASできる状態でも失敗することがあるらしい、仕組みはよくわからん
        // strongはCASできるときは常に成功する
        bool r_try_lock() {
            int expected, desired;
            expected = counter.load(std::memory_order_acquire);
            for (;;) {  // 無限ループの表記法は何でもいいけど、ここはccbenchに乗っ取って
                if (expected != -1) {       //write lockが取られていないなら
                    desired = expected + 1;
                } else {
                    return false;
                }
            }
            // counterの現在の値とexpectedを比較して、trueならcounterをdesiredで置き換え、そうじゃないならexpectedを現在のCounterで上書きする
            // acq_relはacquireとrelease両方の性質を持っているらしい？
            if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
                return true;
            }
        }

        bool w_try_lock() {
            int expected, desired = -1; // write lockを取りたいからdesired = -1
            expected = counter.load(std::memory_order_acquire);
            for (;;) {
                if (expected != 0) {    // 既にwrite/read lockが取られている(write lockはexclusiveなものだから)
                    return false;
                }
                if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return true;
                }
            }
        }

        bool try_upgrade() {    // read lockをwrite lockにupgradeしよう!(自分しかread lockしてないときにしか使えないよ)
            int expected, desired = -1;
            expected = counter.load(std::memory_order_acquire);
            for (;;) {
                if (expected != -1) {   //自分以外がread/write lockを取得しているなら
                    return false;
                }
                if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel)) {
                    return true;
                }
            }
        }

        void r_unlock() {
            counter--;  // もう読んでませんよ～
        }

        void w_unlock() {
            counter++;  // 0(not locked)に戻すため
        }
};

struct Tuple {
    public:
        RWLock lock;
        uint64_t value;
};


Tuple *Table;

struct OperationSet {
    public:
        uint64_t key;
        uint64_t value;
        Tuple *tuple;

        // TODO : operation
};

enum struct Status {
    InFlight,
    Committed,
    Aborted
};

struct TxExecutor {
    public:
        Status status = Status::InFlight;
        std::vector<Task> task_set;
        std::vector<RWLock *> r_lock_list;
        std::vector<RWLock *> w_lock_list;
        std::vector<OperationSet *> read_set;
        std::vector<OperationSet *> write_set;

        void begin() {
            // transactionのinitializeをするよ
            status = Status::InFlight;
        }

        void read(uint64_t key) {
            // keyをtableからreadするよ
            Tuple *tuple = &Table[key];
        }

        void write(uint64_t key) {

        }

        void commit() {

        }

        void abort() {

        }
};

void partTableInit(int thID, int start, int end) {
    printf("thID = %d, %d -> %d\n", thID, start, end);
    for (int i = start; i <= end; i++) {
        Table[i].value = 0;
    }
}

void makeDB() {
    auto maxthread = std::thread::hardware_concurrency();
    // tuple_numを割り切れる最大のthread数を求めるよ
    for (maxthread; maxthread > 0; maxthread--) {
        if (TUPLE_NUM % maxthread == 0) break;
    }
    std::vector<std::thread> th;   // threads
    for (int i = 0; i < maxthread; i++) {
        th.emplace_back(partTableInit, i, i * (TUPLE_NUM / maxthread), (i + 1) * (TUPLE_NUM / maxthread) - 1);
    }
    for (auto &t : th) t.join();
}

void waitForReady(const std::vector<int> &readys) {
    while (true) {
        bool failed = false;
        for (const auto &ready : readys) {
            if (!__atomic_load_n(&ready, __ATOMIC_ACQUIRE)) {
                failed = true;
                break;
            }
        }
        if (!failed) break;
    }
}

void worker(int thID, int &ready, const bool &start, const bool &quit) {
    // &{variable}で、変数を指すアドレスを算出できる
    // 基本的に関数の引数は値渡し(数値を関数内の変数にコピーする)だから、都度変化する変数を参照したいときは&を使って参照渡ししようねってこと
    // startとendにconstを付けているのは、参照渡しだけどこっちの関数でいじられたら困るからガードをかけているってこと？
    Result &myres = std::ref(AllResult[thID]);
    TxExecutor trans;

    // TODO : まだいろいろ書きます

    // threadの足並みが揃っていることを確認(管理)する
    // start = 1になったら一斉に走り出す, それまで待機
    // atomic系の操作はrelease->atomicの順番になっている(はず)
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    while (true) {
        if (!__atomic_load_n(&start, __ATOMIC_ACQUIRE)) break;
    }

    // quitがfalseの間Transactionを生成し続けるよ
    while (true) {
        if (__atomic_load_n(&quit, __ATOMIC_ACQUIRE)) break;
    }


}

int main() {
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    makeDB();

    bool start = false;
    bool quit = false;

    std::vector<int> readys(THREAD_NUM);
    std::vector<std::thread> th;
    for (int i = 0; i < THREAD_NUM; i++) {
        th.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit));
    }
    waitForReady(readys);

    __atomic_store_n(&start, true, __ATOMIC_RELEASE);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    __atomic_store_n(&quit, true, __ATOMIC_RELEASE);

    for (auto &t : th) {
        t.join();
    }

    // TODO : result
    
}