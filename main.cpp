#include "./include/zipf.hh"
#include "./include/random.hh"

#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

// test用に数を少なくしておくよ
#define TUPLE_NUM 10
#define PRE_NUM 10
#define THREAD_NUM 2
#define MAX_OPE 8  
#define SLEEP_POS 7

#define PAGE_SIZE 4096      // memory align用(?)のページサイズ
// #define TUPLE_NUM 1000000   // データ数
// #define THREAD_NUM 12       // Thread数(パソコンの最大Thread数)
// #define PRE_NUM 1000000     // Transactionの件数
#define SKEW_PER 0.0        // なにこれ？zipfで使ってる
#define RW_RATE 50          // Read/Writeの比率
// #define MAX_OPE 16          // 各Preに入るOperationの総数,PRE_NUM * MAX_OPE分だけOperationがあるってことでOK?
// #define SLEEP_POS 15        // Operationの最後にSleepを入れる用？
#define SLEEP_TIME 100      // Operation::SLEEPで寝てる時間

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
                // counterの現在の値とexpectedを比較して、trueならcounterをdesiredで置き換え、そうじゃないならexpectedを現在のCounterで上書きする
                // acq_relはacquireとrelease両方の性質を持っているらしい？
                if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire)) {
                    return true;
                }
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
        RWLock lock;    //atomic<int> counter
        uint64_t value;
};


Tuple *Table;

struct OperationSet {
    public:
        uint64_t key;
        uint64_t value;
        Tuple *tuple;

        // TODO : operation
        OperationSet(uint64_t key, uint64_t value, Tuple *tuple) : key(key), value(value), tuple(tuple) {}
};

enum struct Status {
    InFlight,
    Committed,
    Aborted
};

struct Pre {
    public:
        std::vector<Task> task_set;
};

std::vector<Pre> Pre_tx_set(PRE_NUM);

struct TxExecutor {
    public:
        Status status = Status::InFlight;
        std::vector<Task> task_set;
        std::vector<RWLock *> r_lock_list;
        std::vector<RWLock *> w_lock_list;
        std::vector<OperationSet> read_set;
        std::vector<OperationSet> write_set;

        void begin() {
            // transactionのinitializeをするよ
            status = Status::InFlight;
        }

        void read(uint64_t key) {
            // keyをtableからreadするよ
            Tuple *tuple = &Table[key];
            uint64_t read_value = __atomic_load_n(&tuple->value, __ATOMIC_ACQUIRE);
            read_set.emplace_back(key, read_value, tuple);
            if (tuple->lock.counter != -1) {
                tuple->lock.r_unlock();     // r_try_lockの時点でcounter++されているやつをdecrementする
            }
        }

        void write(uint64_t key) {
            Tuple *tuple = &Table[key];
            __atomic_store_n(&tuple->value, 100, __ATOMIC_RELEASE); //writeするvalueは100固定?
            write_set.emplace_back(key, 100, tuple);
            tuple->lock.w_unlock();
        }

        // commitしたからリスト初期化して使いまわすわよ
        void commit() {
            r_lock_list.clear();
            w_lock_list.clear();
            read_set.clear();
            write_set.clear();
        }

        // 取得しているLockを全部解放するよ
        // 後の処理はCommitと同じだけど勘違いするかもだから冗長だけどこっちでも宣言するよ
        void abort() {
            for (auto &lock : r_lock_list) {
                lock->r_unlock();
            }
            for (auto &lock : w_lock_list) {
                lock->w_unlock();
            }
            r_lock_list.clear();
            w_lock_list.clear();
            read_set.clear();
            write_set.clear();
        }
};

void makeTask(std::vector<Task> &tasks, Xoroshiro128Plus &rand, FastZipf &zipf) {
    tasks.clear();  // vectorの初期化
    for (int i = 0; i < MAX_OPE; i++) {
        uint64_t random_key = zipf();
        assert(random_key < TUPLE_NUM);

        if (i == SLEEP_POS) {
            tasks.emplace_back(Operation::SLEEP, 0);
        }
        // Q : rand.next()のコードが読める気せえへん、乱数生成でOK?
        if ((rand.next() % 100) < RW_RATE) {
            tasks.emplace_back(Operation::READ, random_key);    // Q : readでrandom_keyを渡す理由は？
        } else {
            tasks.emplace_back(Operation::WRITE, random_key);
        }
    }
}

void partTableInit(int thID, int start, int end) {
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
    uint64_t tx_pos = PRE_NUM / THREAD_NUM * thID;  // workerが実行するPre_tx_setの範囲選定用

    // threadの足並みが揃っていることを確認(管理)する
    // start = 1になったら一斉に走り出す, それまで待機
    // atomic系の操作はrelease->atomicの順番になっている(はず)
    __atomic_store_n(&ready, 1, __ATOMIC_RELEASE);
    while (true) {
        if (!__atomic_load_n(&start, __ATOMIC_ACQUIRE)) break;
    }

    std::cout << "thID:" << thID << " start" << std::endl;

    // quitがfalseの間Transactionを実行し続けるよ
    while (true) {
        if (__atomic_load_n(&quit, __ATOMIC_ACQUIRE)) break;
        if (tx_pos >= PRE_NUM / THREAD_NUM * (thID + 1)) return;    // ここでreturnしてるから被りはないけど,端数は?

        // Q : work_txを介さないといけない理由がわからんけど、直接だとcompile errorになる
        Pre &work_tx = std::ref(Pre_tx_set[tx_pos]);
        trans.task_set = work_tx.task_set;

        std::cout << "thID:" << thID << " task[" << tx_pos << "] started" << std::endl;

        tx_pos++;

        RETRY:  // GOTOで飛んでくるところ
        std::cout << "thID:" << thID << " task[" << tx_pos << "] REstarted" << std::endl;
        if (__atomic_load_n(&quit, __ATOMIC_ACQUIRE)) break;
        trans.begin();

        // lockを取得しに行く
        for (auto &task : trans.task_set) {
            Tuple *tuple = &Table[task.key];    // read or write対象のtuple
            int count = 0;                      // 用途不明(後で🤔)
            bool duplicate_flag = false;        // txが既にread or writeしているitemを参照していたら

            switch (task.ope) {
            case Operation::READ:
                // 既に読み込んだことがあるかの確認
                for (auto &r_lock : trans.r_lock_list) {
                    if (r_lock == &tuple->lock) {   // tuple->lockってatomicなcounterじゃないの？これ比較してる？
                        duplicate_flag == true;
                        break;
                    }
                }
                for (auto &w_lock : trans.w_lock_list) {
                    if (w_lock == &tuple->lock) {
                        duplicate_flag = true;
                        break;
                    }
                }
                if (duplicate_flag) break;

                // read lockを試みて、ダメならAbort、行けたらr_lock_listに突っ込んぬ
                if (!tuple->lock.r_try_lock()) {
                    trans.status == Status::Aborted;
                } else {
                    trans.r_lock_list.emplace_back(&tuple->lock);
                }
                break;

            case Operation::WRITE:
                // upgradeできるかの確認(自分だけがread lockを取得している場合)
                for (auto r_lock : trans.r_lock_list) {
                    count++;    // r_listからupgradeしたやつのindexを保持している
                    if (r_lock == &tuple->lock) {
                        if (!tuple->lock.try_upgrade()) {
                            trans.status = Status::Aborted;
                        } else {    // 自分がread lockを取得していて、かつtry_upgradeが通ったら
                            trans.w_lock_list.emplace_back(&tuple->lock);
                            trans.r_lock_list.erase(trans.r_lock_list.begin() + count - 1);
                        }
                    }
                }
                // writeの重複に対する処理
                for (auto w_lock : trans.w_lock_list) {
                    if (w_lock == &tuple->lock) {
                        duplicate_flag = true;
                        break;
                    }
                }
                if (duplicate_flag) break;

                // write lockを試みて、ダメならAbort、行けるならw_lock_listに突っ込もう！
                if (!tuple->lock.w_try_lock()) {
                    trans.status == Status::Aborted;
                } else {
                    trans.w_lock_list.emplace_back(&tuple->lock);
                }
                break;

            case Operation::SLEEP:  //寝とけカス
                break;
            default:
                std::cout << "おい！なんか変だぞ！" << std::endl;
                break;
            }

            if (trans.status == Status::Aborted) {
                trans.abort();
                // std::cout << "thID:" << thID << " aborted" << std::endl;
                goto RETRY; // 本来ならgotoを安易に使うとプログラムの流れが不安定になるから使わない方が良いけど、abortしたTransactionはretryしたいから仕方ないンゴね
            }

        }

        printf("thID:%d | all locks have been acquired\n", thID);

        // lockの確認ができたから処理を行うよ
        for (auto &task : trans.task_set) {
            switch (task.ope)
            {
            case Operation::READ:
                std::cout << "thID:" << thID << " READ" << std::endl;
                trans.read(task.key);
                break;

            case Operation::WRITE:
                std::cout << "thID:" << thID << " WRITE" << std::endl;
                trans.write(task.key);
                break;

            case Operation::SLEEP:
                std::cout << "thID:" << thID << " SLEEP" << std::endl;
                std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_TIME));
                break;

            default:
                std::cout << "おい！なんか変だぞ！！" << std::endl;
                break;
            }
        }

        // commitするぞ！(動作未確認なので後で追う)
        trans.commit();
        myres.commit_cnt++;
    }


}

int main() {
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));

    // randとzipfの初期化
    Xoroshiro128Plus rand;
    FastZipf zipf(&rand, SKEW_PER, TUPLE_NUM);

    makeDB();

    std::cout << "=== makeDB done ===" << std::endl;

    bool start = false;
    bool quit = false;

    for (auto &pre : Pre_tx_set) {
        //vector<vector<Task>>になっている, Pre_tx_setを充足させていく感じ
        makeTask(pre.task_set, rand, zipf);
    }

    // DEBUG : Pre_tx_setの中身確認
    int unchi_index = 0;
    for (auto &i : Pre_tx_set) {
        printf("%2d | ", unchi_index); unchi_index++;
        for (auto &j : i.task_set) {
            printf("%2ld", j.key);
            if (j.ope == Operation::READ) printf("[R] ");
            if (j.ope == Operation::WRITE) printf("[W] ");
            if (j.ope == Operation::SLEEP) printf("[S] ");
        }
        printf("\n");
    }

    std::cout << "=== makeTask done ===" << std::endl;

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