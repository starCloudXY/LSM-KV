#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"
#include <vector>
#define Tiering 0
#define Leveling 1
class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList *memTable;
    std::vector<std::vector<SSTable*>> tablelist;
    uint64_t currentTime;
    std::string dataDir;
    Config configs[10];
    void compactLevel(uint32_t level);

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

    std::string get_in_cache(uint64_t key) override;

    std::string get(uint64_t key) override;
    std::string get_without_cache(uint64_t key)override;
	bool del(uint64_t key) override;

	void reset() override;
};
