#include <iostream>
#include <thread>
#include <vector>
#include <atomic>

#define PAGE_SIZE 4096
#define TUPLE_NUM 1000000

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
            }
        }

        void r_unlock() {
            counter--;  // もう読んでませんよ～
        }

        void w_unlock() {
            counter++;  // 0(not locked)に戻すため
        }
};


struct tuple {
    public:
        int lock;   //RWlockに後で変える
        int value;
};

tuple *Table;

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

int main() {
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(tuple));
    makeDB();

    
}