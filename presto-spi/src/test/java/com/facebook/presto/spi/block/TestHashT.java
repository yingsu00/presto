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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
//import com.facebook.presto.spi.type.BigintType;
//import com.facebook.presto.spi.type.DoubleType;
//import static org.testng.Assert.assertEquals;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.System.arraycopy;
import java.lang.reflect.Field;

import sun.misc.Unsafe;

#define MHASH_M  ((uint64) 0xc6a4a7935bd1e995)
#define MHASH_R 47

    #define MHASH_STEP_1(h, data) \
{ \
  long __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      h = __k * MHASH_M; \
    }

    #define MHASH_STEP(h, data) \
{ \
  long __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      __k *= MHASH_M;  \
      h ^= __k; \
      h *= MHASH_M; \
    }



    #define SLABSIZE (128 * 1024)
#define SLABSHIFT 17
    #define SLABMASK 0x1ffff

    #ifdef SLICES

	#define DECLTABLE(table) \
	Slice[] slices = table.slices;
  
	
#define DECLGET(prefix) Slice prefix##slice; int prefix##offset 

    #define  PREGET(prefix, l)					\
{ prefix##slice = slices[l >> SLABSHIFT]; prefix##offset = l & SLABMASK; }

#define GETL(prefix, field)			\
    prefix##slice.getLong(prefix##offset + field)

    #define SETL(prefix##field, v) \
    prefix##slice.putLong(prefix##offset + field, v)

    #define SLICEREF(n, o) \
    ((n << SLICE_SLABSHIFT) + o)

    #else
	#define DECLGET(prefix) long prefix##ptr
	    #define PREGET(prefix, l)		\
	  prefix##ptr = l
						 #define GETL(prefix, field) \
    unsafe.getLong(prefix##ptr, field)

						 #define SETL(prefix, field, v) \
    unsafe.setLong(prefix##ptr, field, v)
   
    
    #emdif


class TestHash {


      static Unsafe unsafe;

      static {
	  try {
	      // fetch theUnsafe object
	      Field field = Unsafe.class.getDeclaredField("theUnsafe");
	      field.setAccessible(true);
	      unsafe = (Unsafe) field.get(null);
	      if (unsafe == null) {
		  throw new RuntimeException("Unsafe access not available");
	      }
	  }
	  catch (Exception e) {
	      throw new RuntimeException(e);
	  }
      }


    // Generic hash table 
    class HashTable {
	int mask;
	int statusMask;
	long[] bloom;
	long[] status;
	long[] table;
	Slice[] slices = new Slice[16];
	long[] slabs;
	long[] fill = new long[16];
	int currentSlab = -1;
	
	long nextResult(long entry, int offset)
	{
	    DECLGET(a);
	    PREGET(a, entry);
	    return GETL(a, offset);
	}

	// Allocates 'bytes' of contiguous space for hash table payload.
	// Returns a slice index, offset pair or a raw long.
	public long allocBytes(int bytes)
	{
	    if (currentSlab == -1 || fill[currentSlab] + bytes > SLABSIZE) {
		long w = addSlab();
		fill[currentSlab] = bytes;
		return w;
	    }
	    int off = fill[currentSlab];
	    fill[currentSlab] += bytes;
	    return SLICEREF(currentSlab, off);
	}

	long newSlab() {
	    #ifdef SLICES
		++currentSlab;
		if (slices.length <= currentSlab) {
		    int newSize = slices.length * 2;
		    slices = arrays.copyOf(slices, newSize);
		    fill = arrays.copyOf(fill, newSize);
		}
	    Slice s = new Slice(new byte[SLABSIZE]);
	    slices[currentSlab] = s;
	    return SLICEREF(nuSlabs, 0);
#else
    #endif
	}
	
	SetSize(int count) {
	    count *= 1.3;
	    int size = 1024;
	    while (size < count) {
		size *= 2;
	    }
	    mask = size - 1;
	    table = new long[size];
	    status = new long[size / 8];
	    statusMask = mask >> 3;
	    for (i = 0; i <= statusMask) {
		status[i] = -1;
	    }
	}
    }
	
    // Generated class for a key/dependent layout 
    class HashBuild extends ExprContext {

	HashTable table = new HashTable();
	BlockContents k1 = new BlockContents();
	BlockContents k2 = new BlockContents();
	BlockContents d1 = new BlockContents();
	long entryCount = 0;
	
	long hashRow(long row)
	{
	    DECLGET(k);
	    PREGET(k, row);
	    long h;
	    MHASH_STEP_1(h, GETL(k, 0));
	    MHASH_STEP(h, GETL(k, 8));
	    return h;
	}
	
	void addImput(Page page)
	{
	    k1.decodeBlock(page.getBlock(0), mapHolder);
	    k2.decodeBlock(page.getBlock(1), mapHolder);
	    d1.decodeBlock(page.getBlock(2), mapHolder);
	    int positionCount = page.getPositionCount();
	    nullsInBatch = null;
	    int[] k1Map = k1.rowNumberMap;
	    int[] k2Map = k2.rowNumberMap;
	    int[]d1Map = d1.rowNumberMap;
	    addNullFlags(k1.valueIsNull, k1.isIdentityMap ? nul : k1Map, positionCount);
	    addNullFlags(k2.valueIsNull, k2.isIdentityMap ? nul : k2Map, positionCount);


	    DECLTABLE(table);

	    for (int i = 0; i < positionCount; ++i) {
		if (nullsInBatch == null ||  !nullsInBatch[i]) {
		    ++entryCount;
		    long row = table.allocateBytes(32);
		    DECLGET(k);
		    PREGET(k, row);
		    SETL(k,0, k1.longs[k1map[i]]);
		    SETL(k, 8, k2.longs[k1map[i]]);
		    SETL(k, 16, d1.longs[d1map[i]]);
		    SETL(k, 24, -1);
		}
	    }
	    k1.release(mapHolder);
	    k2.release(mapHolder);
	    d1.release(mapHolder);
	}
	void build()
	{
	    table.setSize(entryCount);
	    int batch = 1024;
	    hashes = new long[BATCH];
	    entries = new long[batch];
	    for (int slab = 0; slab <= currentSlab; ++slab) {
		int slabFill = fill[slab];
		for (offset = 0; offset < slabFill; offset += 32) {
		    long entry = SLICEREF(slab, offset);
		    entries[fill] = entry;
		    hashes[fill++] = HashRow(entry);
		    if (fill == batch) {
			insertHashes(hashes, entries, fill)
			    fill = 0;
		    }
		}
	    }
	    insertHashes(hashes, entries);
	}

	void insertHashes(long[] hashes, long[] entries, int fill)
	{
	    int statusMask = table.statusMask;
	    for (int i = 0; i < fill; ++i) {
	    long h = hashes[i] & statusMask;
	    boolean inserted = false;
	    long field = (hashes[i] >> 56) & 0x7f;
	    field |= field << 8;
	    field |= field << 16;
	    field |= field << 32;
	    do {
		long st = status[h];
		long hits = st ^ field;
		hits -= 0x7f7f7f7f7f7f7f7fL;
		hits &= 0x8080808080808080L
		    while (hits != 0) {
			PREGET(b, entries[i];
			       int pos = long.numberOfTrailingZeros(hits) >> 3;
			       PREGET(a, table.table[h * 8 + pos]);
			       if (GETL(a, 0) == GETL(b, 0) && GETL(a, 8) == GETL(b, 8)) {
				   SETL(a, 24, entries[i]);
				   inserted = true;
				   break;
			       }
			       hits = hits &= (hits - 1)
			       }
		    }
	    if (hits == 0) {
		// No matches in the status group, see if can insert.
		st &= 0x8080808080808080;
		if (st != 0) {
		    int pos = long.numberOfTrailingZeros(st) >> 3;
		    table.table[h * 8 + pos] = entries[i];
		    break;
		}
	    }
	    h = (h + 1) & statusMask;
	    } while (!inserted);
	    }
	}
    }
	
    class HashProbe extends ExprContext {
	BlockContents k1 = new BlockContents();
	BlockContents k2 = new BlockContents();
	long hashes[];
	HashTable table;
	int currentInput;
	long nextRow;
	long[] k1d;
	long[] k2d;
	int[] k1Map;
	int[] k2Map;
	int[] candidates;
	int candidateFill;
	int positionCount;
	int[] resultMap;
	int resultFill;
	long[] result1;
	
	public void addInput(Page page) {
	    k1.decodeBlock(page.getBlock(0), mapHolder);
	    k2.decodeBlock(page.getBlock(0), mapHolder);
	    positionCount = page.getPositionCount();
	    nullsInBatch = null;
	    k1Map = k1.rowNumberMap;
	    k2Map = k2.rowNumberMap;
	    addNullFlags(k1.valueIsNull, k1.isIdentityMap ? nul : k1Map, positionCount);
	    addNullFlags(k2.valueIsNull, k2.isIdentityMap ? nul : k2Map, positionCount);
	    if (candidates == null || candidates.length < positionCount) {
		candidates = mapHolder.getIntArray(positionCount);
	    }
	    if (nullsInBatch != null) {
		for (int i; i < positionCount; ++i) {
		    if (nullsInBatch[i]) {
			candidates[candidateFill++] = i;
		    }
		}
	    } else {
		for (int i = 0; i < positionCount; ++i) {
		    candidates[i] = i;
		    candidateFill = positionCount;
		}
	    }
	    for (int i = 0; i <candidateFill; ++i) {
		int row = candidates[i];
		long h;
		MHASH_STEP_1(h, k1d[k1Map[row]]);
		MHASH_STEP(h, k2d[k2Map[row]]);
		hashes[row] = h;
	    }
	}


	public boolean addResult(long entry, int probeRow)
	{
	    resultMap[resultFill] = probeRow;
	    DECLGET(a)
	    PREGET(a, entry);
	    values1[resultFill] = GETL(a, entry, 16);
	    return resultFill >= maxResults;
	}


	public Page getOutput() {
	    while (currentResult != -1) {
		long result = currentResult;
		currentResult = table.nextResult(currentResult, 24);
		if (addResult(result, candidates[currentProbe])) {
		    return finishResult();
		}
	    }

	    #define DECLPROBE(sub) \
		long field##sub;   \
	    long hits##sub; \
	    lomg hash##sub; \
	    int row##sub; \
	    boolean found##sub; \
	    boolean match##sub;


	    #define PREPROBE(sub) \
		row##sub = candidates[i + sub]; \
	    hash##sub = hashes[row##sub]; \
	    field##sub = (hash##sub >> 56) & 0x7f; \
	    hash##sub &= statusMask; \
	    hits##sub = table.status[hash##sub]; \
	    field##sub |= field##sub << 8; \
	    field##sub |= field##sub << 16; \
	    field##sub |= field##sub << 32; \
	}
	
    }

	#define FIRSTPROBE(sub)	      \
	    hits##sub ^= field##sub;  \
	hits##sub -= 0x0101010101010101;
	hits##sub &= 0x8080808080808080;
	if (hits##sub != 0) {
	    pos##sub = long.NumberOfTrailingZeros(hits##sub) >> 3;
	    PREGET(g##sub, table.table[hash##sub * 8 + pos##sub]);
	    match##sub =GETL(g##sub, 0) == k1d[k1Map[row##sub]]
		& GETL(g##sub, 8) == k2d[k2Map[row##sub]];
	}
	
	    
	    #define FULLPROBE(sub) \
{ \
    if (match##sub) { \
	if (addResult(SLICEREF(g##sub), row##sub)) return; \
    } \
    do { \
	while (hits##sub != 0) { \
	    pos##sub = long.NumberOfTrailingZeros(hits##sub) >> 3; \
	    PREGET(g##sub, table.table[hash##sub * 8 + pos##sub]); \
	    if (GETL(g##sub, 0) == k1d[k1Map[row##sub]] && GETL(g##sub, 8) == k2d[k2Map[row##sub]]) { \
		if (addResult(SLICEREF(g##sub), row##sub)) { \
		    return; \
		    found##sub = true; \
		} \
	    } \
	    if (found##sub) break;  \
	    hits##sub &= hits##sub - 1; \
	} \
	if (found##sub) break; \
	hash##sub = (hash##sub + 1) & statusMask; \
    } \
}      

    }
    

