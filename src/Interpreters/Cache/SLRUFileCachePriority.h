#pragma once

#include <list>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/Guards.h>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSizeLimit;
}

namespace DB
{

/// Based on the SLRU algorithm implementation, the record with the lowest priority is stored at
/// the head of the queue, and the record with the highest priority is stored at the tail.
class SLRUFileCachePriority : public IFileCachePriority
{
private:
    class SLRUFileCacheIterator;
    using LRUQueue = std::list<Entry>;
    using SLRUQueueIterator = typename LRUQueue::iterator;

public:
    SLRUFileCachePriority(size_t max_size, size_t max_elements, double size_ratio);

    size_t getSize(const CacheGuard::Lock &) const override { return protected_queue_size + probationary_queue_size; }

    size_t getElementsCount(const CacheGuard::Lock &) const override { return protected_queue_elements_num + probationary_queue_elements_num; }

    Iterator add(KeyMetadataPtr key_metadata, size_t offset, size_t size, const CacheGuard::Lock &) override;

    void pop(const CacheGuard::Lock &) override;

    void removeAll(const CacheGuard::Lock &) override;

    void iterate(IterateFunc && func, const CacheGuard::Lock &) override;

    void shuffle(const CacheGuard::Lock &) override;

private:
    void updateElementsCount(int64_t num, bool is_protected);
    void updateSize(int64_t size, bool is_protected);
    IFileCachePriority::Iterator addImpl(Entry && entry, bool is_protected, const CacheGuard::Lock &);

    LRUQueue protected_queue;
    LRUQueue probationary_queue;

    const size_t protected_queue_size_limit;
    const size_t probationary_queue_size_limit;

    Poco::Logger * log = &Poco::Logger::get("SLRUFileCachePriority");

    std::atomic<size_t> protected_queue_size = 0;
    std::atomic<size_t> protected_queue_elements_num = 0;

    std::atomic<size_t> probationary_queue_size = 0;
    std::atomic<size_t> probationary_queue_elements_num = 0;

    SLRUQueueIterator remove(SLRUQueueIterator it, bool is_protected);
    SLRUQueueIterator increasePriority(SLRUQueueIterator & it, bool is_protected);
};

class SLRUFileCachePriority::SLRUFileCacheIterator : public IFileCachePriority::IIterator
{
public:
    SLRUFileCacheIterator(
        SLRUFileCachePriority * cache_priority_,
        SLRUFileCachePriority::SLRUQueueIterator queue_iter_,
        bool is_protected_);

    const Entry & getEntry() const override { return *queue_iter; }

    Entry & getEntry() override { return *queue_iter; }

    size_t use(const CacheGuard::Lock &) override;

    void remove(const CacheGuard::Lock &) override;

    void invalidate() override;

    void updateSize(int64_t size) override;

private:
    void checkUsable() const;

    SLRUFileCachePriority * cache_priority;
    mutable SLRUFileCachePriority::SLRUQueueIterator queue_iter;
    const bool is_protected;
};

}
