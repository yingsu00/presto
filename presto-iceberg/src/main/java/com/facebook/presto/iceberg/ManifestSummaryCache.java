/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.hive.CacheStatsMBean;
import com.google.common.cache.Cache;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * Caches the manifest summary folded for one (snapshot, schema, spec, predicate, columns) tuple.
 *
 * <p>Folding that summary is the dominant cost of planning against a large table: it walks every
 * data file in the snapshot and decodes the per-file column bounds. A single query asks for it
 * repeatedly, because a new {@code CachingStatsProvider} is created per optimizer pass and its
 * cache is keyed on plan node identity, and repeat queries ask for it again.
 *
 * <p>Every input is part of the key (see {@link ManifestSummaryCacheKey}), so entries are shared
 * across queries, sessions and users without any staleness window. A new commit produces a new
 * snapshot id and therefore a new key rather than a stale hit.
 */
public class ManifestSummaryCache
{
    private final Cache<ManifestSummaryCacheKey, Partition> cache;
    private final CacheStatsMBean cacheStats;
    private final CounterStat foldedFiles = new CounterStat();

    public ManifestSummaryCache(Cache<ManifestSummaryCacheKey, Partition> cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.cacheStats = new CacheStatsMBean(cache);
    }

    /**
     * Returns the cached summary, or computes and caches it. {@code loader} may return null, which
     * is how a scan matching no files is reported; that outcome is not cached, being both cheap to
     * recompute and rare.
     */
    public Optional<Partition> get(ManifestSummaryCacheKey key, Callable<Optional<Partition>> loader)
            throws ExecutionException
    {
        Partition cached = cache.getIfPresent(key);
        if (cached != null) {
            return Optional.of(cached);
        }
        Optional<Partition> loaded;
        try {
            loaded = loader.call();
        }
        catch (Exception e) {
            throw new ExecutionException(e);
        }
        loaded.ifPresent(summary -> cache.put(key, summary));
        return loaded;
    }

    public void invalidateAll()
    {
        cache.invalidateAll();
    }

    @Managed
    @Nested
    public CacheStatsMBean getCacheStats()
    {
        return cacheStats;
    }

    @Managed
    @Nested
    public CounterStat getFoldedFiles()
    {
        return foldedFiles;
    }

    public void recordFoldedFiles(long count)
    {
        foldedFiles.update(count);
    }
}
