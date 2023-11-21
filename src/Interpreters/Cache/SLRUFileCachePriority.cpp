#include <Interpreters/Cache/SLRUFileCachePriority.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/CurrentMetrics.h>
#include <Common/randomSeed.h>
#include <Common/logger_useful.h>
#include <pcg-random/pcg_random.hpp>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheSize;
    extern const Metric FilesystemCacheElements;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SLRUFileCachePriority::SLRUFileCachePriority(
    size_t max_size,
    size_t max_elements,
    double size_ratio)
    : IFileCachePriority(max_size, max_elements)
    , protected_queue_size_limit(static_cast<size_t>(max_size * std::max(0.0, std::min(1.0, size_ratio))))
    , probationary_queue_size_limit(max_size - protected_queue_size_limit)
{
    LOG_DEBUG(
        log, "Using probationary queue size: {}, protected queue size: {}",
        probationary_queue_size_limit, protected_queue_size_limit);
}

IFileCachePriority::Iterator SLRUFileCachePriority::add(
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CacheGuard::Lock & lock)
{
    return addImpl(Entry(key_metadata->key, offset, size, key_metadata), false, lock);
}

IFileCachePriority::Iterator SLRUFileCachePriority::addImpl(Entry && entry, bool is_protected, const CacheGuard::Lock &)
{
    if (entry.size == 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Adding zero size entries to SLRU queue is not allowed "
            "(key: {}, offset: {})", entry.key, entry.offset);
    }

#ifndef NDEBUG
    auto check_queue = [&](const LRUQueue & queue)
    {
        for (const auto & queue_entry : queue)
        {
            /// entry.size == 0 means entry was invalidated.
            if (queue_entry.size != 0
                && queue_entry.key == entry.key && queue_entry.offset == entry.offset)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Attempt to add duplicate queue entry to queue. "
                    "(Key: {}, offset: {}, size: {})",
                    entry.key, entry.offset, entry.size);
            }
        }
    };

    if (is_protected)
        check_queue(protected_queue);
    else
        check_queue(probationary_queue);
#endif

    const size_t size_limit = is_protected ? protected_queue_size_limit : probationary_queue_size_limit;
    const size_t current_size = is_protected ? protected_queue_size : probationary_queue_size;

    if (size_limit && current_size + entry.size > size_limit)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Not enough space to add {}:{} with size {}: current size: {}/{}",
            entry.key, entry.offset, entry.size, current_size, size_limit);
    }

    SLRUQueueIterator iter;
    if (is_protected)
        iter = probationary_queue.insert(probationary_queue.end(), entry);
    else
        iter = protected_queue.insert(probationary_queue.end(), entry);

    updateSize(entry.size, is_protected);
    updateElementsCount(1, is_protected);

    LOG_TEST(
        log, "Added entry into SLRU queue, key: {}, offset: {}, size: {}",
        entry.key, entry.offset, entry.size);

    return std::make_shared<SLRUFileCacheIterator>(this, iter, is_protected);
}

void SLRUFileCachePriority::removeAll(const CacheGuard::Lock &)
{
    LOG_TEST(log, "Removed all entries from SLRU queue");

    updateSize(-protected_queue_size, true);
    updateSize(-probationary_queue_size, false);

    updateElementsCount(-protected_queue_elements_num, true);
    updateElementsCount(-probationary_queue_elements_num, false);

    protected_queue.clear();
    probationary_queue.clear();
}

void SLRUFileCachePriority::pop(const CacheGuard::Lock &)
{
    //TODO
    // remove(queue.begin());
}

SLRUFileCachePriority::SLRUQueueIterator SLRUFileCachePriority::remove(SLRUQueueIterator it, bool is_protected)
{
    /// If size is 0, entry is invalidated, current_elements_num was already updated.
    if (it->size)
    {
        updateSize(-it->size, is_protected);
        updateElementsCount(-1, is_protected);
    }

    LOG_TEST(
        log, "Removed entry from SLRU queue, key: {}, offset: {}, size: {}",
        it->key, it->offset, it->size);

    if (is_protected)
        return protected_queue.erase(it);
    else
        return probationary_queue.erase(it);
}

void SLRUFileCachePriority::updateSize(int64_t size, bool is_protected)
{
    if (is_protected)
        protected_queue_size += size;
    else
        probationary_queue_size += size;

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheSize, size);
}

void SLRUFileCachePriority::updateElementsCount(int64_t num, bool is_protected)
{
    if (is_protected)
        protected_queue_elements_num += num;
    else
        probationary_queue_elements_num += num;

    CurrentMetrics::add(CurrentMetrics::FilesystemCacheElements, num);
}


void SLRUFileCachePriority::iterate(IterateFunc && func, const CacheGuard::Lock &)
{
    for (auto it = queue.begin(); it != queue.end();)
    {
        auto locked_key = it->key_metadata->tryLock();
        if (!locked_key || it->size == 0)
        {
            it = remove(it);
            continue;
        }

        auto metadata = locked_key->tryGetByOffset(it->offset);
        if (!metadata)
        {
            it = remove(it);
            continue;
        }

        if (metadata->size() != it->size)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mismatch of file segment size in file segment metadata "
                "and priority queue: {} != {} ({})",
                it->size, metadata->size(), metadata->file_segment->getInfoForLog());
        }

        auto result = func(*locked_key, metadata);
        switch (result)
        {
            case IterationResult::BREAK:
            {
                return;
            }
            case IterationResult::CONTINUE:
            {
                ++it;
                break;
            }
            case IterationResult::REMOVE_AND_CONTINUE:
            {
                it = remove(it);
                break;
            }
        }
    }
}

SLRUFileCachePriority::SLRUQueueIterator
SLRUFileCachePriority::increasePriority(SLRUQueueIterator & it, bool is_protected)
{
    if (is_protected)
    {
        protected_queue.splice(protected_queue.end(), protected_queue, it);
    }
    else
    {
        if (it->size > protected_queue_size_limit)
        {
            /// This is only possible if protected_queue_size_limit is less than max_file_segment_size,
            /// which is not possible in any realistic cache configuration.
            LOG_DEBUG(
                log,
                "Queue entry {}:{} (size: {}) cannot be moved to protected queue, "
                "because the size of this entry exceeds maximum size of protected queue ({})",
                it->key, it->offset, it->size, protected_queue_size_limit);
        }
        else
        {
            if (protected_queue_size + it->size > protected_queue_size_limit)
            {
            }

            protected_queue.splice(protected_queue.end(), probationary_queue, it);
        }
    }
}

SLRUFileCachePriority::SLRUFileCacheIterator::SLRUFileCacheIterator(
    SLRUFileCachePriority * cache_priority_,
    SLRUFileCachePriority::SLRUQueueIterator queue_iter_,
    bool is_protected_)
    : cache_priority(cache_priority_)
    , queue_iter(queue_iter_)
    , is_protected(is_protected_)
{
}

void SLRUFileCachePriority::SLRUFileCacheIterator::remove(const CacheGuard::Lock &)
{
    checkUsable();
    cache_priority->remove(queue_iter, is_protected);
    queue_iter = SLRUQueueIterator{};
}

void SLRUFileCachePriority::SLRUFileCacheIterator::invalidate()
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Invalidating entry in SLRU queue. Key: {}, offset: {}, previous size: {}",
        queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(-queue_iter->size, is_protected);
    cache_priority->updateElementsCount(-1, is_protected);
    queue_iter->size = 0;
}

void SLRUFileCachePriority::SLRUFileCacheIterator::updateSize(int64_t size)
{
    checkUsable();

    LOG_TEST(
        cache_priority->log,
        "Update size with {} in SLRU queue for key: {}, offset: {}, previous size: {}",
        size, queue_iter->key, queue_iter->offset, queue_iter->size);

    cache_priority->updateSize(size, is_protected);
    queue_iter->size += size;
}

size_t SLRUFileCachePriority::SLRUFileCacheIterator::use(const CacheGuard::Lock &)
{
    checkUsable();
    cache_priority->increasePriority(queue_iter, is_protected);
    return ++queue_iter->hits;
}

void SLRUFileCachePriority::SLRUFileCacheIterator::checkUsable() const
{
    if (queue_iter == SLRUQueueIterator{})
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to use invalid iterator");
}

void SLRUFileCachePriority::shuffle(const CacheGuard::Lock &)
{
    auto shuffle_impl = [](LRUQueue & queue)
    {
        std::vector<SLRUQueueIterator> its;
        its.reserve(queue.size());
        for (auto it = queue.begin(); it != queue.end(); ++it)
            its.push_back(it);
        pcg64 generator(randomSeed());
        std::shuffle(its.begin(), its.end(), generator);
        for (auto & it : its)
            queue.splice(queue.end(), queue, it);
    };
    shuffle_impl(protected_queue);
    shuffle_impl(probationary_queue);
}

}
